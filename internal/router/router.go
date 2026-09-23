/**
 * @file router
 * @description Upstream selection and failover ordering contracts.
 *
 * Responsibilities:
 * - Define how candidate upstreams are ordered for a request
 * - Nothing else: health sampling and per-provider adapters belong to
 *   the upstream module; breaker state is consulted through the circuit
 *   port
 *
 * Candidates are returned in priority order with breaker-open entries
 * excluded (invariant I4). Failover is modeled upstream of this module
 * as a retry attempt against the next candidate. Candidates are
 * upstream instances, not bare IDs: the router owns resolution so
 * callers never need a separate id-to-upstream registry.
 */
package router

import (
	"context"

	"github.com/daftpunkwav/breakwater/internal/upstream"
)

// Router selects candidate upstreams per model.
type Router interface {
	// Candidates lists upstream instances eligible for the model,
	// priority ordered with breaker-open entries already excluded. The
	// first entry is the primary; later entries are failover targets in
	// order.
	Candidates(ctx context.Context, model string) ([]upstream.Upstream, error)
}
