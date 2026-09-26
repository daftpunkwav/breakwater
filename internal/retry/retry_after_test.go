/**
 * @file retry_after_test
 * @description Retry-After support: header parsing, the hint accessor,
 * hint stripping for failover, and the attempt loop preferring the
 * upstream's requested wait over its own backoff.
 */
package retry

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestParseRetryAfter(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	cases := []struct {
		name string
		raw  string
		want time.Duration
	}{
		{"empty", "", 0},
		{"seconds", "3", 3 * time.Second},
		{"fractional", "0.5", 500 * time.Millisecond},
		{"negative", "-5", 0},
		{"zero", "0", 0},
		{"garbage", "soon", 0},
		{"past date", now.UTC().Add(-time.Minute).Format("Mon, 02 Jan 2006 15:04:05 GMT"), 0},
		{"future date", now.Add(time.Minute).UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT"), time.Minute},
		{"padded", " 2 ", 2 * time.Second},
		{"capped", "3600", retryAfterCap},
	}
	for _, tc := range cases {
		if got := ParseRetryAfter(tc.raw, now); got != tc.want {
			t.Errorf("%s: ParseRetryAfter(%q) = %v, want %v", tc.name, tc.raw, got, tc.want)
		}
	}
}

func TestStripRetryAfter(t *testing.T) {
	t.Parallel()
	hinted := &StatusError{StatusCode: 429, RetryAfter: 5 * time.Second}
	stripped, ok := StripRetryAfter(hinted).(*StatusError)
	if !ok {
		t.Fatalf("StripRetryAfter returned %T, want *StatusError", StripRetryAfter(hinted))
	}
	if stripped.StatusCode != 429 || stripped.RetryAfter != 0 {
		t.Fatalf("stripped = %+v, want the status kept and the hint cleared", stripped)
	}
	if hinted.RetryAfter == 0 {
		t.Fatal("StripRetryAfter mutated the original error")
	}

	plain := errors.New("connection reset")
	if got := StripRetryAfter(plain); got != plain {
		t.Fatalf("StripRetryAfter(plain) = %v, want the error unchanged", got)
	}
	unhinted := NewStatusError(503)
	if got := StripRetryAfter(unhinted); got != error(unhinted) {
		t.Fatalf("StripRetryAfter(unhinted) = %v, want the error unchanged", got)
	}
}

func TestDelayHintReadsStatusError(t *testing.T) {
	t.Parallel()
	var c DefaultClassifier
	if got := c.DelayHint(&StatusError{StatusCode: 429, RetryAfter: 7 * time.Second}); got != 7*time.Second {
		t.Fatalf("DelayHint = %v, want 7s", got)
	}
	if got := c.DelayHint(&StatusError{StatusCode: 503}); got != 0 {
		t.Fatalf("DelayHint = %v, want 0 without a hint", got)
	}
	if got := c.DelayHint(errors.New("connection reset")); got != 0 {
		t.Fatalf("DelayHint = %v, want 0 for transport errors", got)
	}
	// A wrapped status error must still be found.
	wrapped := fmt.Errorf("exchange: %w", &StatusError{StatusCode: 429, RetryAfter: time.Second})
	if got := c.DelayHint(wrapped); got != time.Second {
		t.Fatalf("DelayHint(wrapped) = %v, want 1s", got)
	}
}

// TestExecutePrefersUpstreamHint: with no backoff shaped, the only
// delay the loop can wait is the upstream's hint, surfaced through
// OnRetry.
func TestExecutePrefersUpstreamHint(t *testing.T) {
	t.Parallel()
	const hint = 60 * time.Millisecond
	attempts := 0
	var observed time.Duration
	policy := Policy{MaxAttempts: 2}
	start := time.Now()
	err := Execute(context.Background(), policy, nil, DefaultClassifier{},
		func(_ int, _ error, delay time.Duration) { observed = delay },
		func(context.Context, int) error {
			attempts++
			if attempts == 1 {
				return &StatusError{StatusCode: 429, RetryAfter: hint}
			}
			return nil
		})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
	if observed != hint {
		t.Fatalf("OnRetry delay = %v, want the upstream hint %v", observed, hint)
	}
	if elapsed := time.Since(start); elapsed < 50*time.Millisecond {
		t.Fatalf("loop returned after %v, want it to wait out the hint", elapsed)
	}
}

// TestExecuteHintDoesNotWaitForFailover: a hint is stripped when the
// caller rewrites the error for a different upstream, so the loop
// falls back to its own schedule.
func TestExecuteHintDoesNotWaitForFailover(t *testing.T) {
	t.Parallel()
	stripped := StripRetryAfter(&StatusError{StatusCode: 429, RetryAfter: 30 * time.Second})
	if got := stripped.(*StatusError).RetryAfter; got != 0 {
		t.Fatalf("stripped hint = %v, want 0", got)
	}
	// The loop itself must not invent a wait for a hint-less error
	// under an unshaped policy.
	calls := 0
	var observed time.Duration
	err := Execute(context.Background(), Policy{MaxAttempts: 2}, nil, DefaultClassifier{},
		func(_ int, _ error, delay time.Duration) { observed = delay },
		func(context.Context, int) error {
			calls++
			if calls == 1 {
				return stripped
			}
			return nil
		})
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if observed != 0 {
		t.Fatalf("delay = %v, want 0 for a hint-less retryable error", observed)
	}
}

// TestExecuteHintBoundByOverallDeadline: the hint never overrides the
// overall deadline; the loop reports the deadline, not the retry.
func TestExecuteHintBoundByOverallDeadline(t *testing.T) {
	t.Parallel()
	policy := Policy{MaxAttempts: 3, OverallDeadline: 40 * time.Millisecond}
	fn := func(_ context.Context, _ int) error {
		return &StatusError{StatusCode: 429, RetryAfter: 30 * time.Second}
	}
	err := Execute(context.Background(), policy, nil, DefaultClassifier{}, nil, fn)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
}
