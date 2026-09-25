/**
 * @file priority_latency_test
 * @description The latency strategy: measured-cheapest first with the
 * configured order as tie-break, static mode left untouched, and the
 * graceful degradation without a tracker.
 */
package router

import (
	"context"
	"testing"
	"time"
)

func TestCandidatesStaticKeepsBindingOrder(t *testing.T) {
	t.Parallel()
	tr := NewTracker()
	// fast-fallback is measured faster, but static mode ignores scores.
	tr.Record("slow-primary", 5*time.Millisecond, false)
	tr.Record("fast-fallback", 1*time.Millisecond, false)

	rt, err := NewPriority([]Binding{
		{Models: []string{"m1"}, Upstream: stubUp{id: "slow-primary"}},
		{Models: []string{"m1"}, Upstream: stubUp{id: "fast-fallback"}},
	}, WithStrategy(StrategyStatic), WithTracker(tr))
	if err != nil {
		t.Fatalf("router: %v", err)
	}

	candidates, err := rt.Candidates(context.Background(), "m1")
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

	rt, err := NewPriority([]Binding{
		{Models: []string{"m1"}, Upstream: stubUp{id: "slow-primary"}},
		{Models: []string{"m1"}, Upstream: stubUp{id: "fast-fallback"}},
	}, WithStrategy(StrategyLatency), WithTracker(tr))
	if err != nil {
		t.Fatalf("router: %v", err)
	}

	candidates, err := rt.Candidates(context.Background(), "m1")
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

	rt, err := NewPriority([]Binding{
		{Models: []string{"m1"}, Upstream: stubUp{id: "measured"}},
		{Models: []string{"m1"}, Upstream: stubUp{id: "brand-new"}},
	}, WithStrategy(StrategyLatency), WithTracker(tr))
	if err != nil {
		t.Fatalf("router: %v", err)
	}

	candidates, err := rt.Candidates(context.Background(), "m1")
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
	rt, err := NewPriority([]Binding{
		{Models: []string{"m1"}, Upstream: stubUp{id: "u1"}},
		{Models: []string{"m1"}, Upstream: stubUp{id: "u2"}},
	}, WithStrategy(StrategyLatency))
	if err != nil {
		t.Fatalf("router: %v", err)
	}

	candidates, err := rt.Candidates(context.Background(), "m1")
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	if got := candidateIDs(candidates); got[0] != "u1" || got[1] != "u2" {
		t.Fatalf("order = %v, want the configured order without a tracker", got)
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
