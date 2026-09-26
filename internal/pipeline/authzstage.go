/**
 * @file authzstage
 * @description The tier model authorization stage: what the resolved
 * identity may call, enforced before any governance spend and before
 * the cache stage is ever consulted.
 *
 * Responsibilities:
 * - Authorize the request's model through AuthorizeModel, the single
 *   authority the inference handler also applies (defense in depth for
 *   the ungoverned mode), so the two verdicts and envelopes cannot
 *   drift apart
 * - Nothing else: authentication belongs to the auth stage; routing
 *   eligibility (operator switches, breakers) belongs to the router
 *
 * Placement is the security property: directly after the auth stage
 * and before the concurrency, rate-limit and quota stages (a forbidden
 * model must not spend the tenant's budget), and before the cache —
 * the cache key is the request body alone, so a replayed entry would
 * otherwise serve a model the tier's allow/deny decision forbids.
 * Without an identity store this stage is not installed; the inference
 * handler keeps the same fail-closed check for that ungoverned mode.
 */
package pipeline

import (
	"net/http"

	"github.com/daftpunkwav/breakwater/internal/protocol"
)

// AuthorizeModel enforces the request's model gates on the carrier:
// the body is ingested once (idempotent with every later reader), a
// model-less request is a 400 and a model outside the tenant's tier a
// 403 — each rejection rendered in the client's format. The tier rule
// is the fail-closed contract the tier promises: an empty allow list
// allows nothing, deny wins over allow, and a zero tenant (a
// deployment without an identity store) never trips the check. The
// ModelAuthzStage and the inference handler both call this one
// function — deliberate duplication of the enforcement, never of the
// rule.
func AuthorizeModel(w http.ResponseWriter, r *http.Request, carrier *Carrier) bool {
	wire := protocol.WireFor(carrier.Format)
	if !EnsureBody(w, r, carrier) {
		return false
	}
	if carrier.Chat.Model == "" {
		wire.RenderError(w, http.StatusBadRequest, "invalid_request", "model is required")
		return false
	}
	if carrier.Tenant.ID != "" && !carrier.Tenant.Tier.AllowsModel(carrier.Chat.Model) {
		carrier.RejectCode = string(protocol.CodeModelNotAllowed)
		wire.RenderError(w, http.StatusForbidden, string(protocol.CodeModelNotAllowed),
			"the tenant tier does not allow model "+carrier.Chat.Model)
		return false
	}
	return true
}

// ModelAuthzStage returns the tier model authorization stage.
func ModelAuthzStage() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			carrier, ok := RequireCarrier(w, r)
			if !ok {
				return
			}
			if !AuthorizeModel(w, r, carrier) {
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
