/**
 * @file priority_latency_test
 * @description The latency strategy: measured-cheapest first with the
 * configured order as tie-break, static mode left untouched, and the
 * graceful degradation without a tracker.
 */
package router

import (
	"context"
	// nosemgrep: go.lang.security.audit.crypto.math_random.math-random-used -- this test injects a deterministic RNG to pin latency tie-breaks; no key material comes from it
	"math/rand/v2"
	"testing"
	"time"
)

func TestCandidatesStaticKeepsBindingOrder(t *testing.T) {
	t.Parallel()
	tr := NewTracker()
	// fast-fallback is measured faster, but static mode ignores scores.
	tr.Record("slow-primary", 5*time.Millisecond, false)
	tr.Record("fast-fallback", 1*time.Millisecond, false)

	priority, err := NewPriority([]Binding{
		{Models: []string{"m1"}, Upstream: stubUp{id: "slow-primary"}},
		{Models: []string{"m1"}, Upstream: stubUp{id: "fast-fallback"}},
	}, WithStrategy(StrategyStatic), WithTracker(tr))
	if err != nil {
		t.Fatalf("router: %v", err)
	}

	candidates, err := priority.Candidates(context.Background(), "m1")
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	if got := candidateIDs(candidates); got[0] != "slow-primary" {
		t.Fatalf("order = %v, want the configured order under static strategy", got)
	}
}

func TestCandidatesLatencyPrefersFastest(t *testing.T) {
	t.Parallel()
	tr := NewTracker()
	tr.Record("slow-primary", 50*time.Millisecond, false)
	tr.Record("fast-fallback", 5*time.Millisecond, false)

	priority, err := NewPriority([]Binding{
		{Models: []string{"m1"}, Upstream: stubUp{id: "slow-primary"}},
		{Models: []string{"m1"}, Upstream: stubUp{id: "fast-fallback"}},
	}, WithStrategy(StrategyLatency), WithTracker(tr))
	if err != nil {
		t.Fatalf("router: %v", err)
	}

	candidates, err := priority.Candidates(context.Background(), "m1")
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	if got := candidateIDs(candidates); got[0] != "fast-fallback" {
		t.Fatalf("order = %v, want the measured-cheapest first", got)
	}
}

// TestCandidatesLatencyUntriedFirst: score 0 beats any measured
// latency, so a freshly added upstream is explored immediately.
func TestCandidatesLatencyUntriedFirst(t *testing.T) {
	t.Parallel()
	tr := NewTracker()
	tr.Record("measured", 1*time.Millisecond, false)

	priority, err := NewPriority([]Binding{
		{Models: []string{"m1"}, Upstream: stubUp{id: "measured"}},
		{Models: []string{"m1"}, Upstream: stubUp{id: "brand-new"}},
	}, WithStrategy(StrategyLatency), WithTracker(tr))
	if err != nil {
		t.Fatalf("router: %v", err)
	}

	candidates, err := priority.Candidates(context.Background(), "m1")
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	if got := candidateIDs(candidates); got[0] != "brand-new" {
		t.Fatalf("order = %v, want the untried upstream explored first", got)
	}
}

// TestCandidatesLatencyWithoutTrackerDegradesToStatic: the strategy is
// an ordering preference only; missing data must not break routing.
func TestCandidatesLatencyWithoutTrackerDegradesToStatic(t *testing.T) {
	t.Parallel()
	priority, err := NewPriority([]Binding{
		{Models: []string{"m1"}, Upstream: stubUp{id: "u1"}},
		{Models: []string{"m1"}, Upstream: stubUp{id: "u2"}},
	}, WithStrategy(StrategyLatency))
	if err != nil {
		t.Fatalf("router: %v", err)
	}

	candidates, err := priority.Candidates(context.Background(), "m1")
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	if got := candidateIDs(candidates); got[0] != "u1" || got[1] != "u2" {
		t.Fatalf("order = %v, want the configured order without a tracker", got)
	}
}

// TestCandidatesLatencyJittersNearTies: candidates within the tie
// buffer of the best score trade the leading slot per request; the
// runner-up always follows.
func TestCandidatesLatencyJittersNearTies(t *testing.T) {
	t.Parallel()
	tr := NewTracker()
	tr.Record("a", 10*time.Millisecond, false)
	tr.Record("b", 10*time.Millisecond, false)

	build := func(seed uint64) *Priority {
		priority, err := NewPriority([]Binding{
			{Models: []string{"m1"}, Upstream: stubUp{id: "a"}},
			{Models: []string{"m1"}, Upstream: stubUp{id: "b"}},
		}, WithStrategy(StrategyLatency), WithTracker(tr),
			WithRandomSource(rand.New(rand.NewPCG(seed, seed))))
		if err != nil {
			t.Fatalf("router: %v", err)
		}
		return priority
	}

	leaders := map[string]bool{}
	for _, seed := range []uint64{1, 2, 3, 4, 5, 6, 7, 8} {
		candidates, err := build(seed).Candidates(context.Background(), "m1")
		if err != nil {
			t.Fatalf("candidates: %v", err)
		}
		got := candidateIDs(candidates)
		leader, follow := got[0], got[1]
		leaders[leader] = true
		if leader == follow {
			t.Fatalf("order = %v, want both candidates once each", got)
		}
	}
	if !leaders["a"] || !leaders["b"] {
		t.Fatalf("leaders = %v, want both near-tied candidates to lead across seeds", leaders)
	}
}

// TestCandidatesLatencyTieCutHonorsBuffer: a candidate beyond the
// buffer of the best score never leads, whatever the source draws.
func TestCandidatesLatencyTieCutHonorsBuffer(t *testing.T) {
	t.Parallel()
	tr := NewTracker()
	tr.Record("fast", 10*time.Millisecond, false)
	tr.Record("slow", 20*time.Millisecond, false)

	priority, err := NewPriority([]Binding{
		{Models: []string{"m1"}, Upstream: stubUp{id: "fast"}},
		{Models: []string{"m1"}, Upstream: stubUp{id: "slow"}},
	}, WithStrategy(StrategyLatency), WithTracker(tr),
		WithRandomSource(rand.New(rand.NewPCG(1, 1))))
	if err != nil {
		t.Fatalf("router: %v", err)
	}
	for i := 0; i < 20; i++ {
		candidates, err := priority.Candidates(context.Background(), "m1")
		if err != nil {
			t.Fatalf("candidates: %v", err)
		}
		if got := candidateIDs(candidates); got[0] != "fast" {
			t.Fatalf("order = %v; a candidate outside the tie buffer must never lead", got)
		}
	}
}

// TestTieCutIsPureMath: the buffer arithmetic — a zero best score ties
// only the other zeros, everything within 10% of the best ties, the
// first candidate beyond the buffer ends the cut.
func TestTieCutIsPureMath(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		scores []float64
		want   int
	}{
		{"single", []float64{5}, 1},
		{"zeros tie zeros", []float64{0, 0, 4}, 2},
		{"within ten percent", []float64{10, 10.5, 12}, 2},
		{"exactly at the limit", []float64{10, 11}, 2},
		{"beyond the limit", []float64{10, 11.1}, 1},
	}
	for _, tc := range cases {
		if got := tieCut(tc.scores); got != tc.want {
			t.Errorf("%s: tieCut(%v) = %d, want %d", tc.name, tc.scores, got, tc.want)
		}
	}
}

// TestParseStrategy covers the configuration mapping.
func TestParseStrategy(t *testing.T) {
	t.Parallel()

	for _, raw := range []string{"", "static"} {
		got, err := ParseStrategy(raw)
		if err != nil || got != StrategyStatic {
			t.Fatalf("ParseStrategy(%q) = %v, %v; want static", raw, got, err)
		}
	}
	if got, err := ParseStrategy("latency"); err != nil || got != StrategyLatency {
		t.Fatalf("ParseStrategy(latency) = %v, %v; want StrategyLatency", got, err)
	}
	if _, err := ParseStrategy("cheapest"); err == nil {
		t.Fatal("ParseStrategy accepted an unknown strategy")
	}
}
