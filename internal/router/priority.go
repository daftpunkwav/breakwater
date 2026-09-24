/**
 * @file priority
 * @description Static priority routing: per-model candidate lists in
 * configured order, with breaker-open upstreams excluded.
 *
 * Responsibilities:
 * - Resolve a model to its ordered candidate upstreams
 * - Nothing else: failover execution belongs to the relay; breaker
 *   accounting to the circuit port
 *
 * Priority is configuration order: the first entry matching a model is
 * the primary, later matches are failover targets. The wildcard model
 * "*" matches everything, letting one generalist upstream back a
 * thousand models without listing them.
 */
package router

import (
	"context"
	"fmt"

	"github.com/daftpunkwav/breakwater/internal/circuit"
	"github.com/daftpunkwav/breakwater/internal/upstream"
)

// wildcardModel matches every requested model.
const wildcardModel = "*"

// entry binds one upstream to the models it serves.
type entry struct {
	models   map[string]struct{}
	upstream upstream.Upstream
}

// Priority is the static model-to-upstreams router. It is immutable
// after construction and therefore safe for concurrent use.
type Priority struct {
	entries []entry
	breaker circuit.Breaker
}

// PriorityOption customizes a Priority.
type PriorityOption func(*Priority)

// WithBreaker installs the breaker whose open state excludes upstreams
// from candidate lists (advisory pre-filtering; enforcement and
// accounting happen at the attempt).
func WithBreaker(b circuit.Breaker) PriorityOption {
	return func(p *Priority) { p.breaker = b }
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
// the wildcard), in binding order, with breaker-open upstreams excluded.
func (p *Priority) Candidates(ctx context.Context, model string) ([]upstream.Upstream, error) {
	candidates := make([]upstream.Upstream, 0, len(p.entries))
	for _, e := range p.entries {
		_, exact := e.models[model]
		_, wild := e.models[wildcardModel]
		if !exact && !wild {
			continue
		}
		if p.breaker != nil && p.breaker.StateOf(ctx, e.upstream.ID()) == circuit.StateOpen {
			continue
		}
		candidates = append(candidates, e.upstream)
	}
	if len(candidates) == 0 {
		return nil, fmt.Errorf("router: no upstream serves model %q", model)
	}
	return candidates, nil
}
