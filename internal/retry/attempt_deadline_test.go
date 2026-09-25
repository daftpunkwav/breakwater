/**
 * @file attempt_deadline_test
 * @description The attempt loop's remaining caps: the MaxAttempts clamp
 * for a zero policy, the per-attempt timeout, the default classifier
 * fallback, and sleep's contract of "d or until the context ends".
 */
package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestExecuteZeroPolicyGetsExactlyOneAttempt(t *testing.T) {
	t.Parallel()
	attempts := 0
	err := Execute(context.Background(), Policy{}, nil, DefaultClassifier{}, nil, func(context.Context, int) error {
		attempts++
		return errors.New("connection reset")
	})
	if err == nil {
		t.Fatal("expected the failure to surface")
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want the zero policy clamped to 1", attempts)
	}
}

func TestExecuteDefaultsToStandardClassifier(t *testing.T) {
	t.Parallel()
	// A nil classifier falls back to the standard table: the transient
	// transport fault retries, the next attempt succeeds.
	attempts := 0
	err := Execute(context.Background(), Policy{MaxAttempts: 3}, nil, nil, nil, func(_ context.Context, attempt int) error {
		attempts++
		if attempt == 1 {
			return errors.New("connection reset")
		}
		return nil
	})
	if err != nil || attempts != 2 {
		t.Fatalf("err = %v attempts = %d, want success on attempt 2", err, attempts)
	}
}

func TestExecuteBoundsEachAttempt(t *testing.T) {
	t.Parallel()
	attemptDeadlines := make([]time.Duration, 0, 2)
	attempts := 0
	fn := func(ctx context.Context, _ int) error {
		attempts++
		deadline, ok := ctx.Deadline()
		if ok {
			attemptDeadlines = append(attemptDeadlines, time.Until(deadline))
		}
		<-ctx.Done()
		return ctx.Err()
	}
	policy := Policy{MaxAttempts: 2, AttemptTimeout: 20 * time.Millisecond}
	err := Execute(context.Background(), policy, nil, DefaultClassifier{}, nil, fn)

	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want the attempt timeout surfaced", err)
	}
	for i, d := range attemptDeadlines {
		if d <= 0 || d > 2*time.Second {
			t.Fatalf("attempt %d deadline = %v, want roughly the 20ms attempt timeout", i+1, d)
		}
	}
}

func TestExecuteReturnsDeadlineWhenSleepInterrupted(t *testing.T) {
	t.Parallel()
	attempts := 0
	fn := func(context.Context, int) error {
		attempts++
		return errors.New("connection reset")
	}
	// Attempts fail instantly; the 5s backoff ceilings make it
	// overwhelmingly likely that the overall deadline lands inside a
	// backoff sleep. The loop must then surface the context error, not
	// the last attempt error — whatever the jittered delays were.
	policy := Policy{
		MaxAttempts:     10,
		OverallDeadline: 30 * time.Millisecond,
		BackoffInitial:  5 * time.Second,
		BackoffMax:      5 * time.Second,
	}
	err := Execute(context.Background(), policy, nil, DefaultClassifier{}, nil, fn)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
	if attempts >= 10 {
		t.Fatalf("attempts = %d, want the deadline to end the loop before its cap", attempts)
	}
}

func TestSleepWaitsOrYields(t *testing.T) {
	t.Parallel()
	// A live context waits the full duration.
	ctx := context.Background()
	start := time.Now()
	if err := sleep(ctx, 5*time.Millisecond); err != nil {
		t.Fatalf("sleep: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 5*time.Millisecond {
		t.Fatalf("sleep returned after %v, want at least 5ms", elapsed)
	}
	// A zero delay is a context check, never an error on a live context.
	if err := sleep(ctx, 0); err != nil {
		t.Fatalf("zero sleep: %v", err)
	}
	// A cancelled context ends the wait immediately with its error.
	done, cancel := context.WithCancel(context.Background())
	cancel()
	if err := sleep(done, time.Hour); !errors.Is(err, context.Canceled) {
		t.Fatalf("sleep on a cancelled context = %v, want Canceled", err)
	}
}
