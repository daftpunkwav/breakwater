/**
 * @file loop_test
 * @description Attempt loop tests: the caps hold, the first-byte
 * boundary is absolute, and the budget gate denies retries only.
 */
package retry

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// countingClassifier counts consultation and answers from a fixture.
type countingClassifier struct {
	calls     int
	retryable bool
}

func (c *countingClassifier) Retryable(_ error) bool {
	c.calls++
	return c.retryable
}

// failTimes fails with retryable transport errors for the first n
// attempts, then succeeds.
func failTimes(n int, calls *int) AttemptFunc {
	return func(context.Context, int) error {
		*calls++
		if *calls <= n {
			return errors.New("connection reset")
		}
		return nil
	}
}

func fastPolicy(maxAttempts int) Policy {
	return Policy{MaxAttempts: maxAttempts}
}

func TestExecuteSucceedsAfterTransientFailures(t *testing.T) {
	t.Parallel()
	calls := 0
	err := Execute(context.Background(), fastPolicy(3), nil, DefaultClassifier{}, nil, failTimes(2, &calls))
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if calls != 3 {
		t.Fatalf("attempts = %d, want 3", calls)
	}
}

func TestExecuteCapsAttempts(t *testing.T) {
	t.Parallel()
	calls := 0
	fn := func(context.Context, int) error {
		calls++
		return errors.New("connection reset")
	}
	err := Execute(context.Background(), fastPolicy(4), nil, DefaultClassifier{}, nil, fn)
	if err == nil {
		t.Fatal("expected the persistent failure to surface")
	}
	if calls != 4 {
		t.Fatalf("attempts = %d, want 4", calls)
	}
}

func TestExecuteStopsOnTerminalError(t *testing.T) {
	t.Parallel()
	calls := 0
	fn := func(context.Context, int) error {
		calls++
		return NewStatusError(401)
	}
	err := Execute(context.Background(), fastPolicy(5), nil, DefaultClassifier{}, nil, fn)
	if err == nil {
		t.Fatal("expected the terminal error to surface")
	}
	if calls != 1 {
		t.Fatalf("attempts = %d, want 1 for a client fault", calls)
	}
}

func TestExecuteNeverClassifiesCommittedErrors(t *testing.T) {
	t.Parallel()
	classifier := &countingClassifier{retryable: true}
	calls := 0
	fn := func(context.Context, int) error {
		calls++
		return ErrCommitted
	}
	err := Execute(context.Background(), fastPolicy(5), nil, classifier, nil, fn)
	if !errors.Is(err, ErrCommitted) {
		t.Fatalf("err = %v, want ErrCommitted", err)
	}
	if calls != 1 {
		t.Fatalf("attempts = %d, want 1: committed attempts end the loop", calls)
	}
	if classifier.calls != 0 {
		t.Fatalf("classifier consulted %d times, want 0 for committed failures", classifier.calls)
	}
}

func TestExecuteHonorsOverallDeadline(t *testing.T) {
	t.Parallel()
	calls := 0
	fn := func(ctx context.Context, _ int) error {
		calls++
		select {
		case <-time.After(50 * time.Millisecond):
			return errors.New("connection reset")
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	policy := Policy{MaxAttempts: 10, OverallDeadline: 80 * time.Millisecond}
	start := time.Now()
	err := Execute(context.Background(), policy, nil, DefaultClassifier{}, nil, fn)
	if err == nil {
		t.Fatal("expected deadline error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("loop ran %v past its overall deadline", elapsed)
	}
}

func TestExecuteBudgetDeniesRetries(t *testing.T) {
	t.Parallel()
	calls := 0
	fn := func(context.Context, int) error {
		calls++
		return errors.New("connection reset")
	}
	// A budget of zero admits no retry attempt at all.
	budget := NewBudget(0)
	err := Execute(context.Background(), fastPolicy(3), budget, DefaultClassifier{}, nil, fn)
	if !errors.Is(err, ErrBudgetExhausted) {
		t.Fatalf("err = %v, want ErrBudgetExhausted", err)
	}
	if calls != 1 {
		t.Fatalf("attempts = %d, want 1: the first attempt needs no budget", calls)
	}
}

func TestExecuteBudgetReleasesAcrossAttempts(t *testing.T) {
	t.Parallel()
	calls := 0
	budget := NewBudget(1)
	err := Execute(context.Background(), fastPolicy(3), budget, DefaultClassifier{}, nil, failTimes(2, &calls))
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if calls != 3 {
		t.Fatalf("attempts = %d, want 3: slots must be released between attempts", calls)
	}
}

func TestExecuteFiresOnRetryWithAttemptErrorAndDelay(t *testing.T) {
	t.Parallel()
	type retryCall struct {
		attempt int
		err     error
		delay   time.Duration
	}
	var calls []retryCall
	attempts := 0
	policy := Policy{MaxAttempts: 3, BackoffInitial: time.Millisecond, BackoffMax: time.Millisecond}
	err := Execute(context.Background(), policy, nil, DefaultClassifier{},
		func(attempt int, rerr error, delay time.Duration) {
			calls = append(calls, retryCall{attempt, rerr, delay})
		},
		func(context.Context, int) error {
			attempts++
			if attempts <= 2 {
				return errors.New("connection reset")
			}
			return nil
		})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(calls) != 2 {
		t.Fatalf("onRetry fired %d times, want 2 (one per retried failure)", len(calls))
	}
	for i, call := range calls {
		if call.attempt != i+1 {
			t.Errorf("onRetry %d reports attempt %d", i, call.attempt)
		}
		if call.err == nil || !strings.Contains(call.err.Error(), "connection reset") {
			t.Errorf("onRetry %d carries err %v, want the triggering failure", i, call.err)
		}
		if call.delay < 0 || call.delay > time.Millisecond {
			t.Errorf("onRetry %d delay %v outside the [0, BackoffMax] ceiling", i, call.delay)
		}
	}
}

// TestExecuteReleasesBudgetWhenAttemptPanics pins the panic path: a
// retry attempt that panics holds a budget slot, and the slot must
// return even though the panic unwinds past the loop — a recovery
// stage higher in the chain contains the panic, but the budget would
// otherwise leak one slot per panic until every retry is denied.
func TestExecuteReleasesBudgetWhenAttemptPanics(t *testing.T) {
	t.Parallel()
	b := NewBudget(1)
	attempts := 0
	func() {
		defer func() {
			if rec := recover(); rec == nil {
				t.Fatal("expected the attempt panic to propagate to the caller's recovery")
			}
		}()
		_ = Execute(context.Background(), fastPolicy(3), b, DefaultClassifier{}, nil,
			func(context.Context, int) error {
				attempts++
				if attempts == 1 {
					return errors.New("connection reset")
				}
				panic("upstream exploded")
			})
	}()
	if attempts != 2 {
		t.Fatalf("attempts = %d, want the panic to land on attempt 2", attempts)
	}
	if !b.Acquire() {
		t.Fatal("the slot held by the panicking attempt was never released")
	}
}

// TestBackoffCeilingValueClamp pins the value clamp: a BackoffInitial
// whose doubling wraps int64 nanoseconds must clamp to the maximum
// instead of wrapping into a tiny value that silently collapses the
// ceiling — or, uncapped, into no delay at all.
func TestBackoffCeilingValueClamp(t *testing.T) {
	t.Parallel()
	// (2^48 + 1us) shifted 16 wraps to ~65.5us in int64 arithmetic;
	// the clamp must keep the ceiling at the BackoffMax cap instead of
	// handing back the wrapped micro-delay.
	policy := Policy{BackoffInitial: (1<<48)*time.Nanosecond + time.Microsecond, BackoffMax: 2 * time.Second}
	if got := time.Duration(backoffCeiling(policy, 17)); got != 2*time.Second {
		t.Fatalf("wrapped ceiling = %v, want the BackoffMax clamp", got)
	}
	// 2^48 shifted 16 wraps to exactly zero; with BackoffMax unset
	// (uncapped) the clamped doubling must stay a delay, never a silent
	// zero that drops the backoff altogether.
	uncapped := Policy{BackoffInitial: (1 << 48) * time.Nanosecond}
	if got := time.Duration(backoffCeiling(uncapped, 17)); got <= 0 {
		t.Fatalf("uncapped ceiling = %v, want the clamped maximum", got)
	}
}
