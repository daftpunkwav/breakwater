/**
 * @file loop
 * @description The attempt loop: one client request decomposes into
 * bounded attempts against upstreams.
 *
 * Responsibilities:
 * - Run attempts under the three caps: MaxAttempts in count,
 *   AttemptTimeout per attempt, OverallDeadline for all together
 * - Gate every retry beyond the first attempt on the process-wide
 *   in-flight budget (retry storm containment)
 * - Own the first-byte boundary: failures marked committed are returned
 *   untouched, the classifier is never consulted
 * - Nothing else: error classification and outcome accounting belong to
 *   the classifier and the callers
 */
package retry

import (
	"context"
	"errors"
	"fmt"
	"math"
	// nosemgrep: go.lang.security.audit.crypto.math_random.math-random-used -- this file draws only non-cryptographic backoff jitter; no key material comes from it
	"math/rand/v2"
	"time"
)

// ErrBudgetExhausted reports that the process-wide in-flight retry
// budget has no slot for another attempt. The wrapped chain keeps the
// triggering attempt's error visible — operators see what the budget
// was spent fighting, not just that it ran out.
var ErrBudgetExhausted = errors.New("retry: global retry budget exhausted")

// AttemptFunc runs one upstream attempt under the derived attempt
// context. Returning nil means the attempt produced a usable reply; a
// failure already committed to the client must be wrapped with (or be)
// ErrCommitted.
type AttemptFunc func(ctx context.Context, attempt int) error

// OnRetry is the observation hook fired before every backoff sleep;
// nil disables observation.
type OnRetry func(attempt int, err error, delay time.Duration)

// Execute runs the attempt loop against fn and returns the final error:
// nil on success, the triggering error when a retryable failure met a
// cap, the raw failure when it is terminal or committed.
//
// A nil Policy member means "no cap": MaxAttempts below 1 is clamped to
// 1, and zero timeouts and deadlines are absent. Backoff shapes the
// ceiling rather than disabling it — only a zero BackoffMax makes the
// delay zero.
func Execute(ctx context.Context, policy Policy, budget *Budget, classifier Classifier, onRetry OnRetry, fn AttemptFunc) error {
	if classifier == nil {
		classifier = DefaultClassifier{}
	}
	overall := ctx
	var cancel context.CancelFunc
	if policy.OverallDeadline > 0 {
		overall, cancel = context.WithTimeout(ctx, policy.OverallDeadline)
		defer cancel()
	}

	var lastErr error
	for attempt := 1; ; attempt++ {
		if attempt > 1 {
			if budget != nil && !budget.Acquire() {
				// Wrap the failure that wanted the retry: the budget
				// verdict lands on the last attempt's error, and the
				// caller renders both.
				return fmt.Errorf("%w: %w", ErrBudgetExhausted, lastErr)
			}
		}
		// The slot is held for exactly one attempt and released on every
		// exit path, a panic in fn included: recovery stages higher in
		// the chain contain the panic, but this defer runs during the
		// unwind — without it a panicking attempt would permanently
		// consume one budget slot and shrink the cap for every later
		// request.
		err := func() (err error) {
			defer func() {
				if attempt > 1 && budget != nil {
					budget.Release()
				}
			}()
			return runAttempt(overall, policy, fn, attempt)
		}()
		lastErr = err

		if err == nil {
			return nil
		}
		// Bytes already reached the client — the loop's authority
		// ends here, no classification, no further attempt.
		if errors.Is(err, ErrCommitted) {
			return err
		}
		// The overall context is done: the caller or the deadline ended
		// this request. The attempt that was in flight is still the
		// reason it failed, so it stays in the chain: callers that
		// render the last upstream error match on it by identity.
		if overallErr := overall.Err(); overallErr != nil {
			if lastErr != nil {
				return fmt.Errorf("%w: %w", overallErr, lastErr)
			}
			return overallErr
		}
		if attempt >= maxAttempts(policy) || !classifier.Retryable(err) {
			return err
		}

		delay := backoffDelay(policy, attempt)
		// A Retry-After hint from the upstream replaces the computed
		// backoff: the upstream knows its own recovery schedule better
		// than our jitter does. The wait still gets a small upward
		// jitter — never below what the upstream asked for, at most half
		// again as long — so requests that failed together do not all
		// return at the same instant and repeat the collision.
		if dh, ok := classifier.(interface {
			DelayHint(error) time.Duration
		}); ok {
			if hint := dh.DelayHint(err); hint > 0 {
				delay = jitteredHint(hint)
			}
		}
		if onRetry != nil {
			onRetry(attempt, err, delay)
		}
		if err := sleep(overall, delay); err != nil {
			// The deadline ended the backoff sleep. The attempt that
			// asked for the wait is still the reason this request
			// failed, so keep it in the chain: callers that match on
			// the last upstream error (the relay's passthrough
			// renderer) must still recognise it.
			if lastErr != nil {
				return fmt.Errorf("%w: %w", overall.Err(), lastErr)
			}
			return overall.Err()
		}
	}
}

// runAttempt derives the attempt context and runs fn once.
func runAttempt(overall context.Context, policy Policy, fn AttemptFunc, attempt int) error {
	attemptCtx := overall
	var cancel context.CancelFunc
	if policy.AttemptTimeout > 0 {
		attemptCtx, cancel = context.WithTimeout(overall, policy.AttemptTimeout)
		defer cancel()
	}
	return fn(attemptCtx, attempt)
}

// maxAttempts clamps the policy cap: every request gets at least one
// attempt, so zero means one, not "retry forever".
func maxAttempts(policy Policy) int {
	if policy.MaxAttempts < 1 {
		return 1
	}
	return policy.MaxAttempts
}

// backoffDelay computes the full-jitter exponential delay before the
// retry that follows attempt n. Full jitter picks uniformly from
// [0, ceiling), spreading retries instead of synchronizing them.
func backoffDelay(policy Policy, attempt int) time.Duration {
	ceiling := backoffCeiling(policy, attempt)
	if ceiling <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(ceiling))
}

// backoffCeiling is the exponential cap for the retry that follows
// attempt n: the first retry waits at most BackoffInitial, every later
// retry doubles the cap. A zero BackoffInitial leaves the cap at
// BackoffMax. The doubling shift is clamped to 16 bits and the value
// to the int64 maximum, so neither can wrap, and a zero BackoffMax
// means "uncapped" rather than "no delay", so the doubling runs
// unbounded in that case.
func backoffCeiling(policy Policy, attempt int) int64 {
	shift := attempt - 1
	if shift < 0 {
		shift = 0
	}
	if shift > 16 {
		shift = 16
	}
	ceiling := int64(policy.BackoffMax)
	// Clamp before shifting: an absurd BackoffInitial past 2^63ns would
	// otherwise wrap into a tiny or negative value and silently collapse
	// the ceiling to microseconds — or, with an uncapped BackoffMax, to
	// no delay at all.
	exp := int64(policy.BackoffInitial)
	if exp > 0 {
		if limit := int64(math.MaxInt64) >> uint(shift); exp > limit {
			exp = math.MaxInt64
		} else {
			exp <<= uint(shift)
		}
	}
	if exp > 0 && (ceiling == 0 || exp < ceiling) {
		return exp
	}
	return ceiling
}

// sleep waits for d or until ctx ends.
func sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// jitteredHint spreads one upstream's Retry-After wait uniformly over
// [hint, 1.5·hint): the requested floor is always honored, and a
// timeout shared by many requests stops being a synchronized wake-up.
func jitteredHint(hint time.Duration) time.Duration {
	spread := int64(hint) / 2
	if spread <= 0 {
		return hint
	}
	return hint + time.Duration(rand.Int64N(spread))
}
