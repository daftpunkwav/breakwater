/**
 * @file obsmiddleware
 * @description The observation pipeline stage: the single place that
 * sees every finished request.
 *
 * Responsibilities:
 * - Record traffic: in-flight gauge, duration histogram, request
 *   counters by outcome
 * - Emit the async access log entry, including the cache and stream
 *   dimensions the evidence documents audit
 * - Nothing else: stage-level counters (rate limited, cache fetch,
 *   retries) are recorded by their owning stages; this stage never
 *   makes governance decisions
 *
 * Stage order: carrier -> observation -> auth -> ... -> completions.
 * Rejected requests are observed too — this is the outermost
 * business-visible view of the gateway.
 */
package pipeline

import (
	"net/http"
	"time"

	"github.com/daftpunkwav/breakwater/internal/httpserver"
	"github.com/daftpunkwav/breakwater/internal/obs"
)

// ObservationStage returns the metrics and access log stage. Both
// recorders may be nil to disable.
func ObservationStage(metrics *obs.Metrics, sink obs.Sink) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			metrics.InflightAdd(1)
			defer metrics.InflightAdd(-1)

			tee := httpserver.NewCountingTee(w)
			next.ServeHTTP(tee, r)

			duration := time.Since(start)
			carrier := CarrierFrom(r.Context())

			tenant, model, requestID, keyID, rejectCode := "", "", "", "", ""
			if carrier != nil {
				tenant = carrier.Tenant.ID
				keyID = carrier.Tenant.KeyID
				model = carrier.Chat.Model
				requestID = carrier.RequestID
				rejectCode = carrier.RejectCode
			}
			upstream := "-"
			aborted, cacheHit, streamed := false, false, false
			var tokens int64
			errorCode := rejectCode
			if carrier != nil {
				cacheHit = carrier.CacheHit
				if carrier.Relay != nil {
					upstream = carrier.Relay.UpstreamID
					aborted = carrier.Relay.Aborted
					streamed = carrier.Relay.Streamed
					tokens = carrier.Consumed
					// A forward-stage failure refines the rejection code:
					// upstream passthroughs classify by status, gateway
					// envelopes and stream aborts by their code.
					if errorCode == "" && (carrier.Relay.Status < 200 || carrier.Relay.Status > 299) {
						errorCode = carrier.Relay.ErrorCode
					}
				}
			}
			status := tee.Status()
			if status == 0 {
				// No header ever reached the wire: the client walked away
				// (or the handler died) before the response started. The
				// aggregation counts it as a disconnect, never a failure.
				status = obs.StatusClientClosedRequest
			}

			metrics.Request(tenant, model, upstream, status)
			metrics.ObserveDuration(upstream, duration.Seconds())
			if aborted {
				metrics.StreamAborted(upstream)
			}

			if sink != nil {
				sink.Record(obs.Entry{
					Time:      start,
					TenantID:  tenant,
					KeyID:     keyID,
					RequestID: requestID,
					Model:     model,
					Upstream:  upstream,
					Method:    r.Method,
					Path:      r.URL.Path,
					Status:    status,
					Duration:  duration,
					CacheHit:  cacheHit,
					Tokens:    tokens,
					Streamed:  streamed,
					ErrorCode: errorCode,
				})
			}
		})
	}
}
