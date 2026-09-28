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
		{"huge seconds overflow int64 math", "9223372036854775807", retryAfterCap},
		{"huge fractional overflow float math", "99999999999999999999", retryAfterCap},
		{"nan means no hint", "NaN", 0},
		{"infinity folds to the cap", "Inf", retryAfterCap},
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
	stripped := StripRetryAfter(hinted)
	// The clear is in place on purpose: the relay's passthrough renderer
	// matches the loop's final error against its stash by object
	// identity, so a clone here would break the match.
	if stripped != error(hinted) {
		t.Fatalf("StripRetryAfter returned %T, want the same error object", stripped)
	}
	if hinted.StatusCode != 429 || hinted.RetryAfter != 0 {
		t.Fatalf("stripped = %+v, want the status kept and the hint cleared in place", hinted)
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
// OnRetry — never below what the upstream asked for, at most half
// again as long.
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
	if observed < hint || observed > 3*hint/2 {
		t.Fatalf("OnRetry delay = %v, want the upstream hint %v jittered into [%v, %v]",
			observed, hint, hint, 3*hint/2)
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

// TestJitteredHintKeepsTheUpstreamFloor: the hint's wait lands in
// [hint, 1.5·hint) — the requested floor is never shortened, and the
// spread stays bounded so a shared timeout is desynchronized without
// being stretched.
func TestJitteredHintKeepsTheUpstreamFloor(t *testing.T) {
	t.Parallel()
	cases := []time.Duration{
		time.Millisecond, 150 * time.Millisecond, time.Second, 45 * time.Second,
	}
	for _, hint := range cases {
		for i := 0; i < 500; i++ {
			got := jitteredHint(hint)
			if got < hint || got >= 3*hint/2 {
				t.Fatalf("jitteredHint(%v) = %v, want within [%v, %v)", hint, got, hint, 3*hint/2)
			}
		}
	}
	// A hint so small it has no room to spread comes back untouched.
	if got := jitteredHint(time.Nanosecond); got != time.Nanosecond {
		t.Fatalf("jitteredHint(1ns) = %v, want 1ns", got)
	}
}
