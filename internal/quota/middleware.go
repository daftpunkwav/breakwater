/**
 * @file middleware
 * @description The quota pipeline stage: reserve before the chain
 * continues, settle against actual consumption after it returns.
 *
 * Responsibilities:
 * - Reserve the estimated token budget; deny with 402 when the balance
 *   cannot cover it (a drained tenant stops here and costs nothing
 *   further)
 * - Settle or cancel the lease once the outcome is known: zero
 *   consumption cancels (rejections, cache hits), anything else
 *   settles with the consumed count
 * - Nothing else: balance provisioning belongs to the admin surface,
 *   abandoned leases to the sweeper
 *
 * Stage order: auth -> limiter -> quota; this stage reuses the token
 * estimate the limiter stage computed.
 */
package quota

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/daftpunkwav/breakwater/internal/obs"
	"github.com/daftpunkwav/breakwater/internal/pipeline"
	"github.com/daftpunkwav/breakwater/internal/protocol"
)

// Middleware returns the quota stage over a ledger. The metrics
// recorder may be nil to disable.
func Middleware(ledger Ledger, metrics *obs.Metrics) pipeline.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			carrier, ok := pipeline.RequireCarrier(w, r)
			if !ok {
				return
			}
			wire := protocol.WireFor(carrier.Format)

			amount := carrier.Tokens
			if amount <= 0 {
				// A deployment without the limiter stage still needs an
				// estimate; with the standard order this never fires.
				amount = pipeline.EstimateTokens(carrier.Chat, carrier.Tenant.Tier.MaxTokens)
				carrier.Tokens = amount
			}

			lease, err := ledger.Reserve(r.Context(), carrier.Tenant.ID, amount)
			switch {
			case errors.Is(err, ErrInsufficientBalance):
				wire.RenderError(w, http.StatusPaymentRequired, string(protocol.CodeInsufficientQuota),
					"the tenant balance cannot cover the estimated request")
				return
			case err != nil:
				// Fail-closed: a gateway that cannot meter must not give
				// away upstream traffic.
				wire.RenderError(w, http.StatusServiceUnavailable, "governance_unavailable",
					"quota ledger unavailable")
				return
			}
			carrier.Lease = lease.ID
			metrics.QuotaReserved(carrier.Tenant.ID, amount)

			next.ServeHTTP(w, r)

			// Settlement deliberately detaches from request cancellation
			// (invariant I10): a client gone mid-flight still settles by
			// the tokens it consumed. The upstream request itself was
			// cancelled through the request context; only the ledger
			// write outlives it. Anything that still fails falls to the
			// sweeper, which errs on the tenant's side.
			settleCtx := context.WithoutCancel(r.Context())
			if carrier.Consumed <= 0 {
				if err := ledger.Cancel(settleCtx, lease.ID); err != nil {
					slog.Warn("quota cancel failed", "lease", lease.ID, "error", err)
				}
				metrics.QuotaRefunded(carrier.Tenant.ID, amount)
				return
			}
			if refund := amount - carrier.Consumed; refund > 0 {
				metrics.QuotaRefunded(carrier.Tenant.ID, refund)
			}
			if err := ledger.Settle(settleCtx, lease.ID, carrier.Consumed); err != nil {
				slog.Warn("quota settle failed", "lease", lease.ID, "error", err)
			}
		})
	}
}
