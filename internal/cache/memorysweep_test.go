/**
 * @file memorysweep_test
 * @description The throttled in-write expiry sweep: expired entries
 * stop occupying capacity at most one scan per interval, live entries
 * are untouched, and a sweep frees the slots the admission gate would
 * otherwise waste on the dead.
 */
package cache

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// sweepFixture is a cache over a stepped clock: advance moves time.
func sweepFixture(t *testing.T, capacity int) (*Memory, func(time.Duration), func() time.Time) {
	t.Helper()
	var now atomic.Value
	now.Store(time.Unix(0, 0))
	m := NewMemory(WithCapacity(capacity), WithClock(func() time.Time { return now.Load().(time.Time) }))
	return m, func(d time.Duration) { now.Store(now.Load().(time.Time).Add(d)) }, func() time.Time { return now.Load().(time.Time) }
}

// TestMemorySweepReclaimsExpired: a Set past the sweep interval clears
// every expired entry in the same locked pass, keeping live ones.
func TestMemorySweepReclaimsExpired(t *testing.T) {
	t.Parallel()
	m, advance, now := sweepFixture(t, 8)
	ctx := context.Background()

	_ = m.Set(ctx, "short", testEntry(), 20*time.Millisecond)
	_ = m.Set(ctx, "long", testEntry(), time.Hour)
	_ = now

	advance(2 * time.Second) // "short" expires and the sweep window opens
	_ = m.Set(ctx, "fresh", testEntry(), time.Hour)

	if _, err := m.Get(ctx, "short"); err == nil {
		t.Fatal("the expired entry must not be served")
	}
	m.mu.Lock()
	_, shortLeft := m.entries["short"]
	longLeft := m.entries["long"].expires.After(now())
	freshLeft := m.entries["fresh"].expires.After(now())
	m.mu.Unlock()
	if shortLeft {
		t.Fatal("the sweep must delete expired entries, not just hide them")
	}
	if !longLeft || !freshLeft {
		t.Fatal("the sweep must keep live entries")
	}
}

// TestMemorySweepThrottled: the scan runs at most once per interval —
// an expiry between sweeps waits for the next write past the interval.
func TestMemorySweepThrottled(t *testing.T) {
	t.Parallel()
	m, advance, _ := sweepFixture(t, 8)
	ctx := context.Background()

	_ = m.Set(ctx, "a", testEntry(), 20*time.Millisecond) // arms the sweep window
	_ = m.Set(ctx, "b", testEntry(), time.Hour)

	advance(50 * time.Millisecond) // "a" expires, but the window is still armed
	_ = m.Set(ctx, "c", testEntry(), time.Hour)
	m.mu.Lock()
	_, aStillThere := m.entries["a"]
	m.mu.Unlock()
	if !aStillThere {
		t.Fatal("the throttled window must not sweep early")
	}

	advance(2 * time.Second) // the interval elapses
	_ = m.Set(ctx, "d", testEntry(), time.Hour)
	m.mu.Lock()
	_, aLeft := m.entries["a"]
	m.mu.Unlock()
	if aLeft {
		t.Fatal("the next write past the interval must sweep")
	}
}

// TestMemorySweepFeedsAdmissionHonestSlots: with a full cache whose
// entries are all expired, the sweep frees real slots so a cold
// newcomer fits without any admission contest against dead entries.
func TestMemorySweepFeedsAdmissionHonestSlots(t *testing.T) {
	t.Parallel()
	m, advance, _ := sweepFixture(t, 2)
	ctx := context.Background()

	_ = m.Set(ctx, "x", testEntry(), 20*time.Millisecond)
	_ = m.Set(ctx, "y", testEntry(), 20*time.Millisecond)

	advance(2 * time.Second) // both expire and the sweep window opens
	_ = m.Set(ctx, "cold", testEntry(), time.Hour)

	if _, err := m.Get(ctx, "cold"); err != nil {
		t.Fatal("the cold newcomer must fit: the sweep freed the dead slots")
	}
	m.mu.Lock()
	n := len(m.entries)
	m.mu.Unlock()
	if n != 1 {
		t.Fatalf("entries = %d, want exactly the newcomer", n)
	}
}
