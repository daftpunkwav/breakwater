/**
 * @file tracker
 * @description Measured upstream performance for the latency routing
 * strategy: an exchange-latency EWMA per upstream with a consecutive
 * failure penalty.
 *
 * Responsibilities:
 * - Record per-attempt exchange outcomes reported by the relay
 * - Score an upstream so the router can order same-model candidates
 * - Nothing else: the tracker does not exclude anyone — breaker state
 *   and operator switches gate eligibility; this only orders
 *
 * Scoring semantics: the score is milliseconds of exchange latency;
 * lower is better. An upstream with no data yet scores 0 (best), so a
 * newly added upstream is explored immediately — failover and the
 * breaker absorb a bad first impression. Every consecutive failure
 * adds a fixed penalty, so a fast-but-failing upstream sinks below a
 * slower healthy one; the first success clears the penalty entirely.
 */
package router

import (
	"fmt"
	"sync"
	"time"
)

// Strategy selects how the router orders eligible candidates.
type Strategy int

const (
	// StrategyStatic keeps the configured binding order (the default).
	StrategyStatic Strategy = iota
	// StrategyLatency orders eligible candidates by their measured
	// exchange latency, cheapest first, configured order as tie-break.
	StrategyLatency
)

// ParseStrategy maps a configuration string onto a strategy. The empty
// string selects the static order.
func ParseStrategy(s string) (Strategy, error) {
	switch s {
	case "", "static":
		return StrategyStatic, nil
	case "latency":
		return StrategyLatency, nil
	default:
		return StrategyStatic, fmt.Errorf("router: unknown routing strategy %q, want \"static\" or \"latency\"", s)
	}
}

// Tracker tuning: the EWMA weight of a fresh sample and the score
// penalty of one consecutive failure, in milliseconds. The penalty is
// deliberately coarse — it exists to demote failing upstreams, not to
// model their latency.
const (
	ewmaAlpha        = 0.25
	failurePenaltyMS = 1000
)

// Tracker records upstream outcomes and scores them. It is safe for
// concurrent use.
type Tracker struct {
	mu      sync.Mutex
	entries map[string]*trackEntry
}

// trackEntry is one upstream's running score state.
type trackEntry struct {
	ewmaMS   float64
	failures int
}

// NewTracker builds an empty tracker.
func NewTracker() *Tracker {
	return &Tracker{entries: make(map[string]*trackEntry)}
}

// Record folds one exchange outcome into the upstream's score.
func (t *Tracker) Record(id string, latency time.Duration, failed bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	e, ok := t.entries[id]
	if !ok {
		e = &trackEntry{}
		t.entries[id] = e
	}
	if failed {
		e.failures++
		return
	}
	e.failures = 0
	ms := float64(latency.Milliseconds())
	if e.ewmaMS == 0 {
		e.ewmaMS = ms
		return
	}
	e.ewmaMS = ewmaAlpha*ms + (1-ewmaAlpha)*e.ewmaMS
}

// Score reports the upstream's current score in milliseconds; lower is
// better. An upstream with no data scores 0 and is tried first.
func (t *Tracker) Score(id string) float64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	e, ok := t.entries[id]
	if !ok {
		return 0
	}
	return e.ewmaMS + float64(e.failures)*failurePenaltyMS
}
