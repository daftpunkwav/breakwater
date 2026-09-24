/**
 * @file middleware
 * @description The rate limiting pipeline stage: RPM/TPM reservation
 * around the rest of the chain.
 *
 * Responsibilities:
 * - Read the request body once (into the carrier) and derive the token
 *   estimate both reservations are based on
 * - Reserve before next(); reject over-limit requests with 429 and a
 *   Retry-After, never touching an upstream (invariant I1)
 * - Correct the reservation after next() against the tokens actually
 *   consumed — refund only, never surcharge
 * - Enforce the degradation policy: a limiter backend error is
 *   fail-closed (a gateway that cannot limit must not forward)
 *
 * Stage order: auth -> limiter -> quota; the limiter stage owns the
 * single body read the quota stage also relies on.
 */
package limiter

import (
	"net/http"
	"strconv"
	"time"

	"github.com/daftpunkwav/breakwater/internal/obs"
	"github.com/daftpunkwav/breakwater/internal/pipeline"
	"github.com/daftpunkwav/breakwater/internal/protocol"
)

// Middleware returns the rate limiting stage. The metrics recorder
// may be nil to disable.
func Middleware(l Limiter, metrics *obs.Metrics) pipeline.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			carrier, ok := pipeline.RequireCarrier(w, r)
			if !ok {
				return
			}
			wire := protocol.WireFor(carrier.Format)
			if !pipeline.EnsureBody(w, r, carrier) {
				return
			}

			limits := Limits{RPM: carrier.Tenant.Tier.RPM, TPM: carrier.Tenant.Tier.TPM}
			tokens := pipeline.EstimateTokens(carrier.Chat, carrier.Tenant.Tier.MaxTokens)

			decision, err := l.Allow(r.Context(), carrier.Tenant.ID, limits, tokens)
			if err != nil {
				// Fail-closed: governance unavailable means reject (PRD Q4).
				wire.RenderError(w, http.StatusServiceUnavailable, "governance_unavailable",
					"rate limiter unavailable")
				return
			}
			if !decision.Allowed {
				metrics.RateLimited(carrier.Tenant.ID)
				seconds := int64(decision.RetryAfter / time.Second)
				if seconds < 1 {
					seconds = 1
				}
				w.Header().Set("Retry-After", strconv.FormatInt(seconds, 10))
				wire.RenderError(w, http.StatusTooManyRequests, "rate_limit_exceeded",
					"tenant rate limit exceeded")
				return
			}
			carrier.Tokens = tokens

			next.ServeHTTP(w, r)

			// Post-call correction: whatever was not consumed goes back.
			// A client already gone cancels this context; the refund is
			// lost and the bucket stays slightly low — the safe direction.
			if refund := tokens - carrier.Consumed; refund > 0 {
				_ = l.Refund(r.Context(), carrier.Tenant.ID, limits, refund)
			}
		})
	}
}
