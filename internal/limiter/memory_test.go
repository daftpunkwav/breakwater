/**
 * @file memory_test
 * @description In-memory limiter semantics: burst capacity, refill,
 * refund ceiling, per-tenant isolation. These pin the reference
 * behavior the Redis scripts must match.
 */
package limiter

import (
	"context"
	"testing"
	"time"
)

func TestMemoryBurstAndRefill(t *testing.T) {
	t.Parallel()
	m := NewMemory()
	ctx := context.Background()
	limits := Limits{RPM: 60, TPM: 0}

	clock := time.Now()
	m.WithClock(func() time.Time { return clock })

	drainBurst(t, ctx, m, "t", limits, 60, "burst")
	mustReject(t, ctx, m, "t", limits, "post-burst must reject")

	clock = clock.Add(2 * time.Second)
	drainBurst(t, ctx, m, "t", limits, 2, "refill")
	mustReject(t, ctx, m, "t", limits, "beyond refill must reject")
}

func TestMemoryTPMRefund(t *testing.T) {
	t.Parallel()
	m := NewMemory()
	ctx := context.Background()
	limits := Limits{RPM: 0, TPM: 100}

	if d, err := m.Allow(ctx, "t", limits, 60); err != nil || !d.Allowed {
		t.Fatalf("reserve: %v %v", d.Allowed, err)
	}
	if d, err := m.Allow(ctx, "t", limits, 50); err != nil || d.Allowed {
		t.Fatalf("oversized must reject: %v %v", d.Allowed, err)
	}
	if err := m.Refund(ctx, "t", limits, 60); err != nil {
		t.Fatalf("refund: %v", err)
	}
	if d, err := m.Allow(ctx, "t", limits, 100); err != nil || !d.Allowed {
		t.Fatalf("after refund: %v %v", d.Allowed, err)
	}
}

func TestMemoryPerTenantIsolation(t *testing.T) {
	t.Parallel()
	m := NewMemory()
	ctx := context.Background()
	limits := Limits{RPM: 1, TPM: 0}

	if d, err := m.Allow(ctx, "a", limits, 0); err != nil || !d.Allowed {
		t.Fatalf("tenant a: %v %v", d.Allowed, err)
	}
	if d, _ := m.Allow(ctx, "a", limits, 0); d.Allowed {
		t.Fatal("tenant a must be drained")
	}
	if d, err := m.Allow(ctx, "b", limits, 0); err != nil || !d.Allowed {
		t.Fatalf("tenant b must be independent: %v %v", d.Allowed, err)
	}
}

func TestMemoryRetryAfterReflectsDeficit(t *testing.T) {
	t.Parallel()
	m := NewMemory()
	ctx := context.Background()
	// 6000 TPM = 100 tokens/s: the 500-token deficit (1500 wanted, 1000
	// left) waits exactly 5s.
	limits := Limits{RPM: 0, TPM: 6000}

	if d, err := m.Allow(ctx, "t", limits, 5000); err != nil || !d.Allowed {
		t.Fatalf("reserve: %v %v", d.Allowed, err)
	}
	d, err := m.Allow(ctx, "t", limits, 1500)
	if err != nil || d.Allowed {
		t.Fatalf("oversized must reject: %v %v", d.Allowed, err)
	}
	if d.RetryAfter < 4500*time.Millisecond || d.RetryAfter > 5500*time.Millisecond {
		t.Fatalf("retry after = %v, want ~5s", d.RetryAfter)
	}
}

func TestMemoryZeroLimitDisablesCeiling(t *testing.T) {
	t.Parallel()
	m := NewMemory()
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		if d, err := m.Allow(ctx, "t", Limits{}, 1<<20); err != nil || !d.Allowed {
			t.Fatalf("request %d: allowed=%v err=%v", i, d.Allowed, err)
		}
	}
}

// TestMemorySweepRearmsAfterBackwardStep: a wall clock stepped back
// past the horizon must not silence the idle-bucket pass. The stale
// window sits ahead of every reachable now, so the pass re-arms
// against the stepped-back clock and buckets keep aging out.
func TestMemorySweepRearmsAfterBackwardStep(t *testing.T) {
	t.Parallel()
	m := NewMemory()
	ctx := context.Background()
	limits := Limits{RPM: 60, TPM: 0}

	clock := time.Now()
	m.WithClock(func() time.Time { return clock })

	// The pass runs on the first decision and drops a tenant idle past
	// the horizon.
	drainBurst(t, ctx, m, "gone", limits, 1, "create")
	clock = clock.Add(bucketIdleTTL + time.Minute)
	drainBurst(t, ctx, m, "kept", limits, 1, "sweep")
	if _, ok := m.rpm["gone:rpm"]; ok {
		t.Fatal("bucket idle past the horizon survived the sweep")
	}

	// Step the clock back more than the horizon: the window armed
	// before the step now sits past any reachable now. The next
	// decision must re-arm the window rather than wait the stale
	// horizon out, so a bucket created under the stepped-back clock
	// still drops once the clock moves on a horizon.
	clock = clock.Add(-3 * bucketIdleTTL)
	drainBurst(t, ctx, m, "doomed", limits, 1, "create under the stepped-back clock")
	clock = clock.Add(bucketIdleTTL + time.Minute)
	drainBurst(t, ctx, m, "later", limits, 1, "sweep")
	if _, ok := m.rpm["doomed:rpm"]; ok {
		t.Fatal("the backward step silenced the sweep: a bucket idle past the horizon survived")
	}
}
