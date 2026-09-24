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

	for i := 0; i < 60; i++ {
		if d, err := m.Allow(ctx, "t", limits, 0); err != nil || !d.Allowed {
			t.Fatalf("burst request %d: allowed=%v err=%v", i, d.Allowed, err)
		}
	}
	if d, err := m.Allow(ctx, "t", limits, 0); err != nil || d.Allowed {
		t.Fatalf("post-burst must reject: %v %v", d.Allowed, err)
	}

	clock = clock.Add(2 * time.Second)
	for i := 0; i < 2; i++ {
		if d, err := m.Allow(ctx, "t", limits, 0); err != nil || !d.Allowed {
			t.Fatalf("refill request %d: allowed=%v err=%v", i, d.Allowed, err)
		}
	}
	if d, err := m.Allow(ctx, "t", limits, 0); err != nil || d.Allowed {
		t.Fatalf("beyond refill must reject: %v %v", d.Allowed, err)
	}
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
