/**
 * @file tracker_test
 * @description The latency tracker: EWMA smoothing, the consecutive
 * failure penalty, the success reset and the no-data score.
 */
package router

import (
	"sync"
	"testing"
	"time"
)

// TestTrackerUnknownScoresZero: an upstream without data is tried
// first — new providers are explored, not starved.
func TestTrackerUnknownScoresZero(t *testing.T) {
	t.Parallel()
	tr := NewTracker()
	if got := tr.Score("ghost"); got != 0 {
		t.Fatalf("score = %v, want 0 without data", got)
	}
}

// TestTrackerEWMAWeightsFreshSamples: the score moves toward each new
// latency by the alpha weight, not in jumps.
func TestTrackerEWMAWeightsFreshSamples(t *testing.T) {
	t.Parallel()
	tr := NewTracker()

	tr.Record("u1", 100*time.Millisecond, false)
	if got := tr.Score("u1"); got != 100 {
		t.Fatalf("first sample score = %v, want the raw 100", got)
	}
	tr.Record("u1", 200*time.Millisecond, false)
	// 0.25*200 + 0.75*100 = 125
	if got := tr.Score("u1"); got != 125 {
		t.Fatalf("second sample score = %v, want 125", got)
	}
}

// TestTrackerFailurePenaltyDemotes: consecutive failures add a coarse
// penalty and the first success clears it.
func TestTrackerFailurePenaltyDemotes(t *testing.T) {
	t.Parallel()
	tr := NewTracker()

	tr.Record("slow-healthy", 500*time.Millisecond, false)
	tr.Record("fast-failing", 10*time.Millisecond, false)
	tr.Record("fast-failing", 10*time.Millisecond, true)
	tr.Record("fast-failing", 10*time.Millisecond, true)

	if tr.Score("fast-failing") <= tr.Score("slow-healthy") {
		t.Fatalf("failing upstream %v not demoted below healthy %v",
			tr.Score("fast-failing"), tr.Score("slow-healthy"))
	}

	tr.Record("fast-failing", 10*time.Millisecond, false)
	if tr.Score("fast-failing") >= tr.Score("slow-healthy") {
		t.Fatalf("first success did not clear the penalty: %v vs %v",
			tr.Score("fast-failing"), tr.Score("slow-healthy"))
	}
}

// TestTrackerConcurrentRecordAndScore: report goroutines and routing
// readers run against each other; -race flags any unsynchronized pass.
func TestTrackerConcurrentRecordAndScore(t *testing.T) {
	t.Parallel()
	tr := NewTracker()

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				tr.Record("u1", 10*time.Millisecond, j%3 == 0)
			}
		}()
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				_ = tr.Score("u1")
			}
		}()
	}
	wg.Wait()
}
