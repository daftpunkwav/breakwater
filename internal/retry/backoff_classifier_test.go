/**
 * @file backoff_classifier_test
 * @description The retry schedule and the classification table the
 * attempt loop consults: the exponential ceiling, the jitter, and which
 * error shapes deserve another attempt.
 */
package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

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

// TestExecuteKeepsTheLastErrorWhenTheDeadlineEndsTheBackoff: the
// attempt that asked for the wait is still the reason the request
// failed, and callers that render the last upstream error match on it by
// identity.
func TestExecuteKeepsTheLastErrorWhenTheDeadlineEndsTheBackoff(t *testing.T) {
	t.Parallel()
	trigger := errors.New("upstream said 503")
	var calls int
	err := Execute(context.Background(),
		Policy{MaxAttempts: 5, OverallDeadline: 40 * time.Millisecond,
			BackoffInitial: time.Hour, BackoffMax: time.Hour},
		nil, DefaultClassifier{}, nil,
		func(context.Context, int) error {
			calls++
			return trigger
		})
	if calls != 1 {
		t.Fatalf("attempts = %d, want the first backoff to outlive the deadline", calls)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want the deadline to end the loop", err)
	}
	if !errors.Is(err, trigger) {
		t.Fatalf("err = %v, want the triggering upstream error still in the chain", err)
	}
}

// TestExecuteKeepsTheErrorWhenTheDeadlineEndsAnAttempt: the sibling of
// the backoff-sleep case. When the budget runs out while an attempt is
// in flight, that attempt's error is still the reason the request
// failed, and the relay's passthrough renderer matches on it by
// identity — dropping it turns a captured upstream status into a
// generic 502.
func TestExecuteKeepsTheErrorWhenTheDeadlineEndsAnAttempt(t *testing.T) {
	t.Parallel()
	trigger := errors.New("upstream said 502")
	var calls int
	err := Execute(context.Background(),
		Policy{MaxAttempts: 3, OverallDeadline: 60 * time.Millisecond,
			BackoffInitial: time.Millisecond, BackoffMax: 2 * time.Millisecond},
		nil, DefaultClassifier{}, nil,
		func(attemptCtx context.Context, attempt int) error {
			calls++
			if attempt == 1 {
				return trigger
			}
			<-attemptCtx.Done() // burn the rest of the budget
			return trigger
		})
	if calls < 2 {
		t.Fatalf("attempts = %d, want the second attempt to burn the budget", calls)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want the deadline to end the loop", err)
	}
	if !errors.Is(err, trigger) {
		t.Fatalf("err = %v, want the triggering upstream error still in the chain", err)
	}
}
