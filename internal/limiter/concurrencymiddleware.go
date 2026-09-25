/**
 * @file concurrencymiddleware
 * @description The concurrency pipeline stage: one slot per in-flight
 * request, taken after authentication and released when the handler
 * returns.
 *
 * Responsibilities:
 * - Reject requests beyond the identity's concurrency ceiling with
 *   429 concurrency_limit_exceeded, before any rate-limit reservation
 *   or quota lease is taken
 * - Nothing else: the slot rides the handler's return, so a client
 *   disconnect releases through the same path as a finished reply
 */
package limiter

import (
	"net/http"
	"strconv"

	"github.com/daftpunkwav/breakwater/internal/obs"
	"github.com/daftpunkwav/breakwater/internal/pipeline"
	"github.com/daftpunkwav/breakwater/internal/protocol"
)

// concurrencyRetryAfterSeconds is the Retry-After floor the
// concurrency rejection advertises: a slot can free at any moment, so
// anything below one second only invites a spin.
const concurrencyRetryAfterSeconds = 1

// ConcurrencyMiddleware returns the concurrency stage. The metrics
// recorder may be nil to disable. Stage order: auth -> model
// authorization -> concurrency -> limiter — a rejected request must not
// consume the identities' rate budget for work it never did.
func ConcurrencyMiddleware(g *Concurrency, metrics *obs.Metrics) pipeline.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			carrier, ok := pipeline.RequireCarrier(w, r)
			if !ok {
				return
			}
			release, acquired := g.Acquire(carrier.Tenant.ID, carrier.Tenant.Tier.Concurrency)
			if !acquired {
				if metrics != nil {
					metrics.ConcurrencyLimited(carrier.Tenant.ID)
				}
				// The same 429 discipline as the rate-limit stage: the
				// response carries Retry-After guidance. The wait here is
				// not computable — a slot frees when any in-flight
				// request of the tenant finishes — so the floor of one
				// second is the honest minimum: retrying sooner cannot
				// succeed, and the value advises, not promises.
				w.Header().Set("Retry-After", strconv.Itoa(concurrencyRetryAfterSeconds))
				wire := protocol.WireFor(carrier.Format)
				wire.RenderError(w, http.StatusTooManyRequests, "concurrency_limit_exceeded",
					"tenant concurrency limit exceeded; wait for an in-flight request to finish")
				return
			}
			defer release()
			next.ServeHTTP(w, r)
		})
	}
}
