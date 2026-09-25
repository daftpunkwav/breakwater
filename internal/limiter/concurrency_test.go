/**
 * @file concurrency_test
 * @description The concurrency gate: the ceiling holds under
 * contention, release restores capacity, release is idempotent and a
 * zero ceiling admits everyone.
 */
package limiter

import (
	"sync"
	"sync/atomic"
	"testing"
)

func TestConcurrencyCeilingHolds(t *testing.T) {
	t.Parallel()
	g := NewConcurrency()

	// The invariant under contention is not "some attempt fails" (that
	// depends on scheduling) but "the concurrent held count never
	// exceeds the ceiling".
	const ceiling = 4
	var inFlight, peak atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 40; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, ok := g.Acquire("t1", ceiling)
			if !ok {
				return
			}
			cur := inFlight.Add(1)
			for {
				p := peak.Load()
				if cur <= p || peak.CompareAndSwap(p, cur) {
					break
				}
			}
			inFlight.Add(-1)
			release()
		}()
	}
	wg.Wait()

	if got := peak.Load(); got > ceiling {
		t.Fatalf("peak in-flight = %d, ceiling = %d", got, ceiling)
	}
	if g.InFlight("t1") != 0 {
		t.Fatalf("in-flight = %d after all releases, want 0", g.InFlight("t1"))
	}
}

func TestConcurrencyReleaseRestoresAndIsIdempotent(t *testing.T) {
	t.Parallel()
	g := NewConcurrency()

	releases := make([]func(), 0, 3)
	for i := 0; i < 3; i++ {
		release, ok := g.Acquire("t1", 3)
		if !ok {
			t.Fatal("acquire within cap failed")
		}
		releases = append(releases, release)
	}
	if _, ok := g.Acquire("t1", 3); ok {
		t.Fatal("acquire beyond the ceiling succeeded")
	}

	releases[0]()
	if g.InFlight("t1") != 2 {
		t.Fatalf("in-flight = %d, want 2 after one release", g.InFlight("t1"))
	}
	releases[0]() // idempotent: a double release must not corrupt the count
	if g.InFlight("t1") != 2 {
		t.Fatalf("double release corrupted the count: %d", g.InFlight("t1"))
	}

	releases[1]()
	releases[2]()
	if _, ok := g.Acquire("t1", 3); !ok {
		t.Fatal("capacity did not recover after releases")
	}
}

// TestConcurrencyZeroMeansUnlimited: a ceiling of zero is the
// documented "disabled" value.
func TestConcurrencyZeroMeansUnlimited(t *testing.T) {
	t.Parallel()
	g := NewConcurrency()
	for i := 0; i < 100; i++ {
		release, ok := g.Acquire("t1", 0)
		if !ok {
			t.Fatal("unlimited ceiling rejected")
		}
		release()
	}
	if g.InFlight("t1") != 0 {
		t.Fatalf("unlimited ceiling tracked slots: %d", g.InFlight("t1"))
	}
}

// TestConcurrencyPerTenantIsolation: one identity exhausting its slots
// never touches another's.
func TestConcurrencyPerTenantIsolation(t *testing.T) {
	t.Parallel()
	g := NewConcurrency()
	for i := 0; i < 2; i++ {
		release, ok := g.Acquire("busy", 2)
		if !ok {
			t.Fatal("acquire failed")
		}
		defer release()
	}
	if _, ok := g.Acquire("busy", 2); ok {
		t.Fatal("busy tenant exceeded its ceiling")
	}
	if release, ok := g.Acquire("other", 2); !ok {
		t.Fatal("another tenant was blocked by the busy tenant's slots")
	} else {
		release()
	}
}
