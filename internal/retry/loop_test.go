/**
 * @file loop_test
 * @description Attempt loop tests: the caps hold (I5), the first-byte
 * boundary is absolute (I6), and the budget gate denies retries only.
 */
package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

// countingClassifier counts consultation and answers from a fixture.
type countingClassifier struct {
	calls     int
	retryable bool
}

func (c *countingClassifier) Retryable(err error) bool {
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
		t.Fatalf("attempts = %d, want 4 (I5)", calls)
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
		t.Fatalf("attempts = %d, want 1: committed attempts end the loop (I6)", calls)
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

func TestClassifierTable(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		err       error
		retryable bool
	}{
		{"nil", nil, false},
		{"too many requests", NewStatusError(429), true},
		{"server error", NewStatusError(503), true},
		{"bad request", NewStatusError(400), false},
		{"unauthorized", NewStatusError(401), false},
		{"payment required", NewStatusError(402), false},
		{"canceled", context.Canceled, false},
		{"deadline", context.DeadlineExceeded, true},
		{"plain transport", errors.New("connection refused"), true},
	}
	var c DefaultClassifier
	for _, tc := range cases {
		if got := c.Retryable(tc.err); got != tc.retryable {
			t.Errorf("%s: retryable = %v, want %v", tc.name, got, tc.retryable)
		}
	}
}

func TestBackoffStaysUnderCeiling(t *testing.T) {
	t.Parallel()
	policy := Policy{BackoffInitial: 10 * time.Millisecond, BackoffMax: 40 * time.Millisecond}
	for attempt := 1; attempt <= 10; attempt++ {
		if d := backoffDelay(policy, attempt); d > policy.BackoffMax || d < 0 {
			t.Fatalf("attempt %d: delay %v outside [0, %v]", attempt, d, policy.BackoffMax)
		}
	}
	if d := backoffDelay(Policy{}, 2); d != 0 {
		t.Fatalf("unshaped policy produced delay %v, want 0", d)
	}
}

func TestBackoffCeilingGrowsFromInitial(t *testing.T) {
	t.Parallel()
	policy := Policy{BackoffInitial: 10 * time.Millisecond, BackoffMax: 40 * time.Millisecond}
	want := []time.Duration{10, 20, 40, 40, 40} // double, then cap at max
	for i, w := range want {
		if got := time.Duration(backoffCeiling(policy, i+1)); got != w*time.Millisecond {
			t.Fatalf("retry %d ceiling = %v, want %v (the sequence must grow from BackoffInitial)", i+1, got, w*time.Millisecond)
		}
	}
	// Without an initial the cap is the max from the first retry on.
	if got := backoffCeiling(Policy{BackoffMax: 30 * time.Millisecond}, 1); got != int64(30*time.Millisecond) {
		t.Fatalf("initial-less ceiling = %v, want 30ms", time.Duration(got))
	}
	// Without a max the doubling is uncapped until the shift clamp.
	if got := backoffCeiling(Policy{BackoffInitial: time.Second}, 3); got != int64(4*time.Second) {
		t.Fatalf("uncapped ceiling = %v, want 4s", time.Duration(got))
	}
}
