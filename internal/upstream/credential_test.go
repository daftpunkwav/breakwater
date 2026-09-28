/**
 * @file credential_test
 * @description The credential ring: rotation across picks, exclusion
 * by the caller, retirement liveness, and the revive that restores the
 * full ring.
 */
package upstream

import (
	"sync"
	"testing"
)

// TestRingRotatesAcrossPicks: consecutive picks walk the ring in order
// and wrap, so concurrent requests spread across the credentials.
func TestRingRotatesAcrossPicks(t *testing.T) {
	ring := newCredentialRing([]string{"a", "b", "c"})
	got := make([]int, 0, 6)
	for i := 0; i < 6; i++ {
		got = append(got, ring.pick(nil))
	}
	want := []int{0, 1, 2, 0, 1, 2}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("picks = %v, want %v", got, want)
		}
	}
}

// TestRingHonorsExclusions: an excluded credential is skipped, and the
// exclusion is per call — a clean pick uses it again.
func TestRingHonorsExclusions(t *testing.T) {
	ring := newCredentialRing([]string{"a", "b", "c"})
	if got := ring.pick([]int{0}); got != 1 {
		t.Fatalf("pick excluding 0 = %d, want 1", got)
	}
	if got := ring.pick([]int{0}); got != 2 {
		t.Fatalf("second pick excluding 0 = %d, want 2", got)
	}
	if got := ring.pick(nil); got != 0 {
		t.Fatalf("clean pick = %d, want 0 again", got)
	}
}

// TestRingDropsExclusionWhenStarved: when every alive credential is
// excluded the scan falls back to reuse instead of failing — one
// request must not starve behind its own walk.
func TestRingDropsExclusionWhenStarved(t *testing.T) {
	ring := newCredentialRing([]string{"a", "b"})
	if got := ring.pick([]int{0, 1}); got < 0 {
		t.Fatalf("pick with the full ring excluded = %d, want a reused credential", got)
	}
}

// TestRingRetireAndAlive: retirement drops the credential from the
// pick, liveness counts what remains, and retiring the last one
// empties the ring.
func TestRingRetireAndAlive(t *testing.T) {
	ring := newCredentialRing([]string{"a", "b", "c"})
	if !ring.retire(1) {
		t.Fatal("first retire must report newly retired")
	}
	if ring.retire(1) {
		t.Fatal("second retire of the same credential must report false")
	}
	if ring.retire(9) {
		t.Fatal("an unknown index must report false")
	}
	if got := ring.alive(); got != 2 {
		t.Fatalf("alive = %d, want 2", got)
	}
	for i := 0; i < 4; i++ {
		if got := ring.pick(nil); got == 1 {
			t.Fatal("a retired credential must not be picked")
		}
	}
	ring.retire(0)
	ring.retire(2)
	if got := ring.alive(); got != 0 {
		t.Fatalf("alive = %d, want 0 after retiring everything", got)
	}
	if got := ring.pick(nil); got != -1 {
		t.Fatalf("pick on an empty ring = %d, want -1", got)
	}
}

// TestRingRevive: revive restores every retirement and the pick order
// continues from the cursor.
func TestRingRevive(t *testing.T) {
	ring := newCredentialRing([]string{"a", "b"})
	ring.retire(0)
	ring.retire(1)
	ring.revive()
	if got := ring.alive(); got != 2 {
		t.Fatalf("alive after revive = %d, want 2", got)
	}
	if got := ring.pick(nil); got < 0 {
		t.Fatalf("pick after revive = %d, want a live credential", got)
	}
}

// TestRingConcurrentPicksStayInBounds: concurrent picks and retires
// never produce an out-of-range index, and while at least one
// credential stays alive the pick never fails.
func TestRingConcurrentPicksStayInBounds(t *testing.T) {
	ring := newCredentialRing([]string{"a", "b", "c", "d"})
	var mu sync.Mutex
	retired := map[int]bool{}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				got := ring.pick(nil)
				if got < 0 || got >= 4 {
					t.Errorf("pick out of range: %d", got)
					return
				}
				if j%50 == 0 {
					mu.Lock()
					// Retire each credential at most once and keep at
					// least one alive, so the pick always has an answer.
					if len(retired) < 3 && !retired[got] {
						retired[got] = true
						ring.retire(got)
					}
					mu.Unlock()
				}
			}
		}()
	}
	wg.Wait()
	if got := ring.alive(); got < 1 {
		t.Fatalf("alive = %d, want the guarded ring to keep one credential", got)
	}
}
