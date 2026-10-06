/**
 * @file affinity_test
 * @description The prefix affinity index under a fixed clock: seeding
 * on misses, deepest-match promotion, eligibility filtering, TTL
 * expiry and the node ceiling.
 */
package affinity

import (
	"strings"
	"testing"
	"time"
)

// clock is a controllable clock for the index.
type clock struct{ at time.Time }

func (c *clock) now() time.Time { return c.at }

func (c *clock) advance(d time.Duration) { c.at = c.at.Add(d) }

// newTestIndex returns an index on a fixed clock at a known time.
func newTestIndex(ttl time.Duration) (*Index, *clock) {
	c := &clock{at: time.Unix(0, 0)}
	return NewIndex(ttl, c.now), c
}

// seededIndex returns an index with "prefix" already recorded under
// head a.
func seededIndex(ttl time.Duration) (*Index, *clock) {
	idx, c := newTestIndex(ttl)
	idx.Pick("m", "prefix", []string{"a", "b"})
	return idx, c
}

// wantNodes asserts the model trie holds exactly want recorded nodes.
func wantNodes(t *testing.T, idx *Index, want int) {
	t.Helper()
	if got := idx.models["m"].nodes; got != want {
		t.Fatalf("nodes = %d, want %d", got, want)
	}
}

func TestPickSeedsOnMissAndMatchesNextTime(t *testing.T) {
	idx, _ := newTestIndex(time.Minute)
	// First pick: no state, order stands, but the decision is recorded.
	if got := idx.Pick("m", "shared prompt text", []string{"a", "b"}); got != -1 {
		t.Fatalf("first pick = %d, want -1", got)
	}
	// Second identical pick: the recorded head is eligible, no swap
	// needed (it already leads).
	if got := idx.Pick("m", "shared prompt text", []string{"a", "b"}); got != -1 {
		t.Fatalf("second pick = %d, want -1", got)
	}
	// The recorded prefix pulls a later candidate to the head.
	if got := idx.Pick("m", "shared prompt text", []string{"b", "a"}); got != 1 {
		t.Fatalf("pick = %d, want 1 (promote a)", got)
	}
}

func TestMatchFollowsSharedChunkDepth(t *testing.T) {
	idx, _ := newTestIndex(time.Minute)
	shared := strings.Repeat("x", chunkSize)
	idx.Pick("m", shared+"tail one", []string{"a", "b"})
	idx.Pick("m", shared+"tail two", []string{"a", "b"})
	// Both requests share only the first chunk; the head was recorded
	// twice, so the order stands.
	if got := idx.Pick("m", shared+"tail three", []string{"a", "b"}); got != -1 {
		t.Fatalf("pick = %d, want -1", got)
	}
	// A prompt recorded under b alone promotes b even though a leads.
	idx.Pick("m", strings.Repeat("y", chunkSize*2), []string{"a", "b"})
	if got := idx.Pick("m", strings.Repeat("y", chunkSize*2), []string{"a", "b"}); got != -1 {
		t.Fatalf("replay pick = %d, want -1", got)
	}
	if got := idx.Pick("m", strings.Repeat("y", chunkSize*2), []string{"b", "a"}); got != 1 {
		t.Fatalf("pick = %d, want 1", got)
	}
}

func TestExcludedUpstreamCannotWin(t *testing.T) {
	idx, _ := seededIndex(time.Minute)
	// a holds the prefix but is not eligible this round: no hit, order
	// stands, and the miss records the current head.
	if got := idx.Pick("m", "prefix", []string{"b"}); got != -1 {
		t.Fatalf("pick = %d, want -1", got)
	}
	// With both eligible again, the deeper record (b from the miss)
	// and the older one (a) tie at depth one; the router's order wins.
	if got := idx.Pick("m", "prefix", []string{"b", "a"}); got != -1 {
		t.Fatalf("pick = %d, want -1", got)
	}
	if got := idx.Pick("m", "prefix", []string{"a", "b"}); got != -1 {
		t.Fatalf("pick = %d, want -1", got)
	}
}

func TestDeepestMatchWinsOverShallowerPreference(t *testing.T) {
	idx, _ := newTestIndex(time.Minute)
	idx.Pick("m", "short", []string{"a", "b"})
	idx.Pick("m", "short and longer", []string{"b"})
	// b holds the longer recorded prefix; a only the shorter one. The
	// deeper match promotes b regardless of router order.
	if got := idx.Pick("m", "short and longer", []string{"a", "b"}); got != 1 {
		t.Fatalf("pick = %d, want 1 (promote b)", got)
	}
}

func TestTTLExpiryReleasesAffinity(t *testing.T) {
	idx, c := seededIndex(time.Minute)
	if got := idx.Pick("m", "prefix", []string{"b", "a"}); got != 1 {
		t.Fatalf("pick = %d, want 1", got)
	}
	c.advance(2 * time.Minute)
	// The expired pick sweeps the stale record and re-seeds under its
	// own head: order stands.
	if got := idx.Pick("m", "prefix", []string{"b", "a"}); got != -1 {
		t.Fatalf("pick after ttl = %d, want -1", got)
	}
	// Only the fresh re-seed survives: if a's stale record had lasted,
	// it would lead this pick at index 0 instead of promoting b.
	if got := idx.Pick("m", "prefix", []string{"a", "b"}); got != 1 {
		t.Fatalf("pick = %d, want 1 (only b's fresh record survives)", got)
	}
	wantNodes(t, idx, 1)
}

func TestSharedPrefixKeepsInteriorAlive(t *testing.T) {
	idx, c := newTestIndex(time.Minute)
	head := strings.Repeat("h", chunkSize)
	tail := head + strings.Repeat("a", chunkSize*3) // four chunks
	// A four-chunk chain under a, fresh at t0.
	idx.Pick("m", tail, []string{"a", "b"})
	// Half a TTL later a head-only pick records b on chunk one; the
	// tail extensions keep their t0 stamps.
	c.advance(time.Minute / 2)
	idx.Pick("m", head, []string{"b"})
	// Past the tail's age but inside chunk one's: the sweep prunes the
	// three stale tail nodes and stops at the fresh interior, and the
	// head-only pick cannot re-extend the chain.
	c.advance(time.Minute/2 + time.Second)
	if got := idx.Pick("m", head, []string{"b", "a"}); got != -1 {
		t.Fatalf("pick = %d, want -1 (b leads)", got)
	}
	if got := idx.models["m"].nodes; got != 1 {
		t.Fatalf("nodes after tail expiry = %d, want 1", got)
	}
	// A full-depth pick re-extends the chain from the survivor.
	if got := idx.Pick("m", tail, []string{"b", "a"}); got != -1 {
		t.Fatalf("pick = %d, want -1", got)
	}
	if got := idx.models["m"].nodes; got != 4 {
		t.Fatalf("nodes after re-extension = %d, want 4", got)
	}
}

func TestNodeCeilingStopsRecording(t *testing.T) {
	idx, _ := newTestIndex(time.Hour)
	// Distinct full-length chains, one per pick, until the trie sits
	// exactly at its node ceiling: 8192 nodes / 512 chunks per chain.
	for i := 0; i < maxNodes/maxChunks; i++ {
		idx.Pick("m", strings.Repeat(string(rune('A'+i)), maxChunks*chunkSize), []string{"a", "b"})
	}
	wantNodes(t, idx, maxNodes)
	// A new prompt cannot record: the ceiling holds, the pick degrades
	// to strategy order.
	if got := idx.Pick("m", strings.Repeat("!", chunkSize), []string{"b", "a"}); got != -1 {
		t.Fatalf("pick at ceiling = %d, want -1", got)
	}
	wantNodes(t, idx, maxNodes)
	// Existing prefixes keep working at the ceiling.
	if got := idx.Pick("m", strings.Repeat("A", maxChunks*chunkSize), []string{"b", "a"}); got != 1 {
		t.Fatalf("pick = %d, want 1 (deep match survives)", got)
	}
}

func TestModelsAreIsolated(t *testing.T) {
	idx, _ := newTestIndex(time.Minute)
	idx.Pick("m1", "prefix", []string{"a", "b"})
	// The same prompt on another model has no recorded prefix.
	if got := idx.Pick("m2", "prefix", []string{"b", "a"}); got != -1 {
		t.Fatalf("pick = %d, want -1", got)
	}
}

func TestNilIndexAndEmptyInputsAreSafe(t *testing.T) {
	var nilIdx *Index
	if got := nilIdx.Pick("m", "p", []string{"a"}); got != -1 {
		t.Fatalf("nil pick = %d, want -1", got)
	}
	idx, _ := newTestIndex(time.Minute)
	if got := idx.Pick("m", "p", nil); got != -1 {
		t.Fatalf("no-candidate pick = %d, want -1", got)
	}
	if got := idx.Pick("m", "", []string{"a", "b"}); got != -1 {
		t.Fatalf("empty-prompt pick = %d, want -1", got)
	}
}

func TestNewIndexRejectsDisabledTTL(t *testing.T) {
	if idx := NewIndex(0, nil); idx != nil {
		t.Fatal("zero ttl must disable the index")
	}
	if idx := NewIndex(-time.Second, nil); idx != nil {
		t.Fatal("negative ttl must disable the index")
	}
}

func TestConcurrentPicksStayConsistent(_ *testing.T) {
	idx, _ := newTestIndex(time.Hour)
	done := make(chan struct{})
	for i := 0; i < 8; i++ {
		go func(i int) {
			defer func() { done <- struct{}{} }()
			for j := 0; j < 200; j++ {
				ids := []string{"a", "b"}
				if (i+j)%2 == 0 {
					ids = []string{"b", "a"}
				}
				idx.Pick("m", "concurrent prefix", ids)
			}
		}(i)
	}
	for i := 0; i < 8; i++ {
		<-done
	}
}
