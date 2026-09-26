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
			aborted, cacheHit := false, false
			errorCode := rejectCode
			if carrier != nil {
				cacheHit = carrier.CacheHit
				if carrier.Relay != nil {
					upstream = carrier.Relay.UpstreamID
					aborted = carrier.Relay.Aborted
					// A forward-stage failure refines the rejection code:
					// upstream passthroughs classify by status, gateway
					// envelopes and stream aborts by their code.
					if errorCode == "" && (carrier.Relay.Status < 200 || carrier.Relay.Status > 299) {
						errorCode = carrier.Relay.ErrorCode
					}
				}
			}

			metrics.Request(tenant, model, upstream, tee.Status())
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
					Status:    tee.Status(),
					Duration:  duration,
					CacheHit:  cacheHit,
					ErrorCode: errorCode,
				})
			}
		})
	}
}
