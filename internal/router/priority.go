/**
 * @file priority
 * @description Per-model candidate lists behind the router port: the
 * configured order by default, or ordered by measured exchange latency
 * under the latency strategy, with ineligible upstreams excluded.
 *
 * Responsibilities:
 * - Resolve a model to its candidate upstreams and order them (static
 *   configured order, or the latency tracker's measured order with the
 *   near-tie leader draw)
 * - Nothing else: failover execution belongs to the relay; breaker
 *   accounting to the circuit port; the latency measurement itself to
 *   the tracker
 *
 * Binding order is configuration order: the first entry matching a
 * model is the primary, later matches are failover targets. The
 * wildcard model "*" matches everything, letting one generalist
 * upstream back a thousand models without listing them.
 */
package router

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"

	"github.com/daftpunkwav/breakwater/internal/circuit"
	"github.com/daftpunkwav/breakwater/internal/upstream"
)

// wildcardModel matches every requested model.
const wildcardModel = "*"

// latencyTieBuffer is the fraction of the best measured score within
// which candidates count as tied for the first slot under the latency
// strategy. Near-equals trade blows per request instead of pinning
// one upstream with everything.
const latencyTieBuffer = 0.10

// ErrUnavailable reports that the model has bound upstreams but none
// of them is currently eligible — every one is circuit-open or
// operator-disabled. Callers map it to 503; the model-not-found 404 is
// reserved for models with no binding at all.
var ErrUnavailable = errors.New("router: every upstream for the model is unavailable")

// entry binds one upstream to the models it serves.
type entry struct {
	models   map[string]struct{}
	upstream upstream.Upstream
}

// Priority is the model-to-upstreams router. Binding order is
// immutable after construction; the optional switch, strategy and
// latency tracker overlay runtime state on top of it. Safe for
// concurrent use.
type Priority struct {
	entries  []entry
	breaker  circuit.Breaker
	control  *Switch
	strategy Strategy
	tracker  *Tracker
	// rng, when set, drives the near-tie leader draw under the latency
	// strategy; nil falls back to the global source. Injected for tests.
	rng *rand.Rand
}

// PriorityOption customizes a Priority.
type PriorityOption func(*Priority)

// WithBreaker installs the breaker whose open state excludes upstreams
// from candidate lists (advisory pre-filtering; enforcement and
// accounting happen at the attempt).
func WithBreaker(b circuit.Breaker) PriorityOption {
	return func(p *Priority) { p.breaker = b }
}

// WithSwitch installs the runtime traffic control. A model disabled by
// an operator fails the candidate lookup with ErrDisabled; a disabled
// upstream drops out of the candidate list like a breaker-open one.
func WithSwitch(s *Switch) PriorityOption {
	return func(p *Priority) { p.control = s }
}

// WithStrategy sets the candidate ordering. StrategyStatic (the
// default) keeps the configured order; StrategyLatency sorts the
// eligible candidates by their measured exchange latency, keeping the
// configured order as the tie-break.
func WithStrategy(s Strategy) PriorityOption {
	return func(p *Priority) { p.strategy = s }
}

// WithTracker installs the latency tracker consulted by
// StrategyLatency. Without a tracker the latency strategy degrades to
// the static order.
func WithTracker(t *Tracker) PriorityOption {
	return func(p *Priority) { p.tracker = t }
}

// WithRandomSource installs the source for the near-tie leader draw;
// nil (the default) uses the global source.
func WithRandomSource(rng *rand.Rand) PriorityOption {
	return func(p *Priority) { p.rng = rng }
}

// NewPriority builds a router from ordered model bindings. Bindings
// without models or upstreams are rejected at assembly time.
func NewPriority(bindings []Binding, opts ...PriorityOption) (*Priority, error) {
	p := &Priority{}
	for _, opt := range opts {
		opt(p)
	}
	for _, b := range bindings {
		if b.Upstream == nil {
			return nil, fmt.Errorf("router: binding for models %v has no upstream", b.Models)
		}
		if len(b.Models) == 0 {
			return nil, fmt.Errorf("router: binding for upstream %s lists no models", b.Upstream.ID())
		}
		models := make(map[string]struct{}, len(b.Models))
		for _, m := range b.Models {
			models[m] = struct{}{}
		}
		p.entries = append(p.entries, entry{models: models, upstream: b.Upstream})
	}
	return p, nil
}

// Binding binds one upstream to the models it serves.
type Binding struct {
	// Models are the model identifiers served; "*" is the wildcard.
	Models []string
	// Upstream is the serving adapter instance.
	Upstream upstream.Upstream
}

// Candidates implements Router: every upstream bound to the model (or
// the wildcard), in binding order, with operator-disabled and
// breaker-open upstreams excluded. A model disabled by an operator
// fails with ErrDisabled. When the model has bindings but none of them
// is currently eligible, the error is ErrUnavailable, not a
// no-binding failure.
func (p *Priority) Candidates(ctx context.Context, model string) ([]upstream.Upstream, error) {
	if p.control != nil && !p.control.ModelEnabled(model) {
		return nil, ErrDisabled
	}
	candidates := make([]upstream.Upstream, 0, len(p.entries))
	bound := 0
	for _, e := range p.entries {
		_, exact := e.models[model]
		_, wild := e.models[wildcardModel]
		if !exact && !wild {
			continue
		}
		bound++
		if p.control != nil && !p.control.UpstreamEnabled(e.upstream.ID()) {
			continue
		}
		if p.breaker != nil && p.breaker.StateOf(ctx, e.upstream.ID()) == circuit.StateOpen {
			continue
		}
		candidates = append(candidates, e.upstream)
	}
	if len(candidates) == 0 {
		if bound > 0 {
			return nil, ErrUnavailable
		}
		return nil, fmt.Errorf("router: no upstream serves model %q", model)
	}
	if p.strategy == StrategyLatency && p.tracker != nil {
		// Snapshot the scores once per candidate and sort the pairs
		// together: the tie cut below must read the same ordering the
		// sort produced, and the fixed snapshot means scores cannot
		// shift mid-sort.
		scored := make([]scoredCandidate, len(candidates))
		for i, c := range candidates {
			scored[i] = scoredCandidate{upstream: c, score: p.tracker.Score(c.ID())}
		}
		sort.SliceStable(scored, func(i, j int) bool {
			return scored[i].score < scored[j].score
		})
		scores := make([]float64, len(scored))
		for i, s := range scored {
			candidates[i], scores[i] = s.upstream, s.score
		}
		// Near-tie exploration: candidates within a buffer of the best
		// score are tied for the lead; which of them goes first is
		// drawn per request, so sustained traffic spreads across
		// near-equals while distinctly slower ones keep the strict
		// order behind them.
		if cut := tieCut(scores); cut > 1 {
			if j := p.pickTied(cut); j != 0 {
				candidates[0], candidates[j] = candidates[j], candidates[0]
			}
		}
	}
	return candidates, nil
}

// scoredCandidate pairs a candidate with its snapshot score, keeping
// the sort and the tie cut on one consistent ordering.
type scoredCandidate struct {
	upstream upstream.Upstream
	score    float64
}

// tieCut reports how many of the ascending-scored candidates count as
// tied for the best score: everything within latencyTieBuffer of it.
// A zero best score — untried upstreams — ties exactly the other
// untried ones; scores are never negative.
func tieCut(sorted []float64) int {
	limit := sorted[0] + sorted[0]*latencyTieBuffer
	cut := 1
	for cut < len(sorted) && sorted[cut] <= limit {
		cut++
	}
	return cut
}

// pickTied draws one of the tied slots; the injected source keeps the
// draw reproducible in tests, the global source costs nothing to run.
func (p *Priority) pickTied(cut int) int {
	if p.rng != nil {
		return p.rng.IntN(cut)
	}
	return rand.IntN(cut)
}
