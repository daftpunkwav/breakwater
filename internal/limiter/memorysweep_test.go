/**
 * @file memorysweep_test
 * @description The idle-bucket sweep: it reclaims buckets the process
 * will not see again without ever changing a decision, so the in-memory
 * backend retains its state on the same schedule as the Redis one.
 */
package limiter

import (
	"context"
	"math"
	"sync/atomic"
	"testing"
	"time"
)

// sweepFixture is a limiter over a stepped clock: advance moves time,
// now reads it.
func sweepFixture(t *testing.T) (*Memory, func(time.Duration), func() time.Time) {
	t.Helper()
	var clock atomic.Value
	clock.Store(time.Unix(0, 0))
	m := NewMemory()
	m.WithClock(func() time.Time { return clock.Load().(time.Time) })
	return m, func(d time.Duration) { clock.Store(clock.Load().(time.Time).Add(d)) },
		func() time.Time { return clock.Load().(time.Time) }
}

// TestMemorySweepDropsIdleBuckets: a tenant that stops sending traffic
// releases its buckets at the first sweep past the horizon, while a
// tenant still in traffic keeps them — with the balance it really has,
// not a fresh one, or the sweep would hand out allowance the tenant
// already spent.
func TestMemorySweepDropsIdleBuckets(t *testing.T) {
	t.Parallel()
	m, advance, _ := sweepFixture(t)
	ctx := context.Background()
	limits := Limits{RPM: 60, TPM: 1000}

	// The first Allow arms the sweep window; "idle" is seeded here and
	// never touched again.
	if d, err := m.Allow(ctx, "idle", limits, 10); err != nil || !d.Allowed {
		t.Fatalf("seed idle tenant: allowed=%v err=%v", d.Allowed, err)
	}

	// "busy" spends its whole allowance and is touched again inside the
	// window the sweep will run in: it is the tenant the sweep must not
	// release.
	for i := 0; i < 60; i++ {
		if d, err := m.Allow(ctx, "busy", limits, 0); err != nil || !d.Allowed {
			t.Fatalf("drain busy tenant: allowed=%v err=%v", d.Allowed, err)
		}
	}
	advance(30 * time.Second)
	if d, err := m.Allow(ctx, "busy", limits, 0); err != nil || !d.Allowed {
		t.Fatalf("top up busy tenant: allowed=%v err=%v", d.Allowed, err)
	}

	// The window is due: the next Allow sweeps. "idle" has been
	// untouched for longer than the horizon and goes; "busy" was
	// touched inside it and keeps what it has.
	advance(90 * time.Second)
	if d, err := m.Allow(ctx, "other", limits, 0); err != nil || !d.Allowed {
		t.Fatalf("sweep trigger: allowed=%v err=%v", d.Allowed, err)
	}

	if _, held := m.rpm["idle:rpm"]; held {
		t.Error("the idle tenant's RPM bucket must be released")
	}
	if _, held := m.tpm["idle:tpm"]; held {
		t.Error("the idle tenant's TPM bucket must be released")
	}
	busy, held := m.rpm["busy:rpm"]
	if !held {
		t.Fatal("a tenant in traffic must keep its RPM bucket")
	}
	// Thirty tokens refilled, one spent. A bucket the sweep released
	// and recreated would start full at sixty and hand the tenant an
	// allowance it never earned.
	if math.Abs(busy.tokens-29) > 1e-9 {
		t.Fatalf("busy bucket holds %v tokens, want 29: a kept bucket keeps its real balance", busy.tokens)
	}
	if _, held := m.tpm["busy:tpm"]; !held {
		t.Error("a tenant in traffic must keep its TPM bucket")
	}
}

// TestMemorySweepWaitsForFullRefill: the sweep may only release a
// bucket that has refilled back to capacity. A rate slow enough that
// the refill outlasts the window is what makes the distinction
// observable at all — the spent allowance of a drained tenant must
// still be there when the bucket is retained, or an early sweep would
// mint free requests.
func TestMemorySweepWaitsForFullRefill(t *testing.T) {
	t.Parallel()
	// A bucket refills from empty to capacity in a minute at any rate
	// (the rate is capacity per minute), so the horizon must outlast
	// that: a bucket released while it is still short of full would
	// hand the tenant an allowance it never earned.
	if bucketIdleTTL < time.Minute {
		t.Fatalf("idle horizon = %s, want at least the minute a drained bucket needs to refill", bucketIdleTTL)
	}
	m, advance, _ := sweepFixture(t)
	ctx := context.Background()
	// 6 RPM is one token per 10s, so a drained bucket needs 60s to refill
	// and still holds only three tokens halfway through the horizon.
	limits := Limits{RPM: 6, TPM: 0}

	// The first Allow arms the sweep window and the burst drains the
	// bucket to zero.
	for i := 0; i < 6; i++ {
		if d, err := m.Allow(ctx, "t", limits, 0); err != nil || !d.Allowed {
			t.Fatalf("burst %d: allowed=%v err=%v", i, d.Allowed, err)
		}
	}

	advance(30 * time.Second)
	if d, err := m.Allow(ctx, "t", limits, 0); err != nil || !d.Allowed {
		t.Fatalf("first refilled token: allowed=%v err=%v", d.Allowed, err)
	}

	held, ok := m.rpm["t:rpm"]
	if !ok {
		t.Fatal("a bucket short of full must not be swept")
	}
	// Three refilled tokens, one spent. A bucket recreated instead of
	// kept would start full at six and read five.
	if math.Abs(held.tokens-2) > 1e-9 {
		t.Fatalf("bucket holds %v tokens, want 2 — a retained bucket must keep its real balance", held.tokens)
	}

	// The sweep runs while the bucket is still short of full: it was
	// touched inside the horizon, so it stays — with the two tokens it
	// really has, not with a fresh allowance.
	advance(bucketIdleTTL - 30*time.Second)
	if d, err := m.Allow(ctx, "other", limits, 0); err != nil || !d.Allowed {
		t.Fatalf("sweep trigger: allowed=%v err=%v", d.Allowed, err)
	}
	held, ok = m.rpm["t:rpm"]
	if !ok {
		t.Fatal("a bucket short of full must survive the sweep")
	}
	if math.Abs(held.tokens-2) > 1e-9 {
		t.Fatalf("bucket holds %v tokens, want 2 after the sweep", held.tokens)
	}
}

// TestMemorySweepThrottled: the pass runs at most once per horizon, so
// a burst of requests in the same window costs one scan, not one per
// request.
func TestMemorySweepThrottled(t *testing.T) {
	t.Parallel()
	m, _, now := sweepFixture(t)
	ctx := context.Background()

	for i := 0; i < 50; i++ {
		if d, err := m.Allow(ctx, "t", Limits{RPM: 1000, TPM: 1000}, 1); err != nil || !d.Allowed {
			t.Fatalf("request %d: allowed=%v err=%v", i, d.Allowed, err)
		}
	}
	armed := m.sweptAt
	if want := now().Add(bucketIdleTTL); !armed.Equal(want) {
		t.Fatalf("sweptAt = %v, want %v", armed, want)
	}
	if _, held := m.rpm["t:rpm"]; !held {
		t.Error("a tenant in traffic must keep its buckets inside the window")
	}
}
