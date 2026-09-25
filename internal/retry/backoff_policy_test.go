/**
 * @file backoff_policy_test
 * @description The backoff shape math at its extremes: the zero-value
 * policy combinations, the shift clamp and the doubling overflow
 * fallback.
 */
package retry

import (
	"testing"
	"time"
)

func TestBackoffDelayBoundsForZeroValuePolicies(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		policy Policy
	}{
		{"fully zero", Policy{}},
		{"initial without max", Policy{BackoffInitial: 5 * time.Millisecond}},
		{"max without initial", Policy{BackoffMax: 10 * time.Millisecond}},
	}
	for _, tc := range cases {
		for attempt := 1; attempt <= 6; attempt++ {
			if got := backoffDelay(tc.policy, attempt); got < 0 {
				t.Fatalf("%s attempt %d: negative delay %v", tc.name, attempt, got)
			}
		}
	}
	// With no shape at all the delay degenerates to zero: a retry without
	// backoff still retries immediately.
	if got := backoffDelay(Policy{}, 2); got != 0 {
		t.Fatalf("fully zero policy delay = %v, want 0", got)
	}
}

func TestBackoffCeilingShiftClamp(t *testing.T) {
	t.Parallel()
	// Deep into a retry sequence the doubling stops: the shift clamps at
	// 16 and the cap takes over.
	policy := Policy{BackoffInitial: time.Second, BackoffMax: time.Hour}
	if got := time.Duration(backoffCeiling(policy, 18)); got != time.Hour {
		t.Fatalf("ceiling at attempt 18 = %v, want the BackoffMax clamp", got)
	}
}

func TestBackoffCeilingOverflowFallsBackToMax(t *testing.T) {
	t.Parallel()
	// 48h doubled 16 times overflows int64 nanoseconds; the ceiling falls
	// back instead of producing a negative delay.
	policy := Policy{BackoffInitial: 48 * time.Hour, BackoffMax: time.Minute}
	if got := time.Duration(backoffCeiling(policy, 18)); got != time.Minute {
		t.Fatalf("overflowed ceiling = %v, want the BackoffMax fallback", got)
	}
}
