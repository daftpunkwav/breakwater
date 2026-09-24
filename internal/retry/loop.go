/**
 * @file loop
 * @description The attempt loop: one client request decomposes into
 * bounded attempts against upstreams.
 *
 * Responsibilities:
 * - Run attempts under the three caps: MaxAttempts in count,
 *   AttemptTimeout per attempt, OverallDeadline for all together
 * - Gate every retry beyond the first attempt on the process-wide
 *   in-flight budget (retry storm containment, invariant I5)
 * - Own the first-byte boundary: failures marked committed are returned
 *   untouched, the classifier is never consulted (invariant I6)
 * - Nothing else: error classification and outcome accounting belong to
 *   the classifier and the callers
 */
package retry

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"
)

// ErrBudgetExhausted reports that the process-wide in-flight retry
// budget has no slot for another attempt. Callers fail fast; wrapping
// keeps the triggering error visible.
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
// 1, zero timeouts and deadlines are absent, zero backoff shapes fall
// back to no delay.
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

	for attempt := 1; ; attempt++ {
		if attempt > 1 {
			if budget != nil && !budget.Acquire() {
				return ErrBudgetExhausted
			}
		}
		err := runAttempt(overall, policy, fn, attempt)
		if attempt > 1 && budget != nil {
			budget.Release()
		}

		if err == nil {
			return nil
		}
		// I6: bytes already reached the client — the loop's authority
		// ends here, no classification, no further attempt.
		if errors.Is(err, ErrCommitted) {
			return err
		}
		// The overall context is done: the caller or the deadline ended
		// this request; the error belongs to the context, not the last
		// attempt.
		if overallErr := overall.Err(); overallErr != nil {
			return overallErr
		}
		if attempt >= maxAttempts(policy) || !classifier.Retryable(err) {
			return err
		}

		delay := backoffDelay(policy, attempt)
		if onRetry != nil {
			onRetry(attempt, err, delay)
		}
		if err := sleep(overall, delay); err != nil {
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
// retry doubles the cap, never past BackoffMax. A zero BackoffInitial
// leaves the cap at BackoffMax; an overflowed doubling falls back to it.
func backoffCeiling(policy Policy, attempt int) int64 {
	shift := attempt - 1
	if shift < 0 {
		shift = 0
	}
	if shift > 16 {
		shift = 16
	}
	ceiling := int64(policy.BackoffMax)
	if exp := int64(policy.BackoffInitial) << uint(shift); exp > 0 && (ceiling == 0 || exp < ceiling) {
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
