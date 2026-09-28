/**
 * @file obsmiddleware
 * @description The observation pipeline stage: the single place that
 * sees every finished request.
 *
 * Responsibilities:
 * - Record traffic: in-flight gauge, duration histogram, request
 *   counters by outcome
 * - Emit the async access log entry, including the cache and stream
 *   dimensions the stability report reads
 * - Nothing else: stage-level counters (rate limited, cache fetch,
 *   retries) are recorded by their owning stages; this stage never
 *   makes governance decisions
 *
 * Stage order: carrier -> request id -> format -> observation -> auth ->
 * ... -> completions. Rejected requests are observed too: this is the
 * first stage that sees the outcome of a business request, so no
 * governance rejection escapes it.
 */
package pipeline

import (
	"net/http"
	"time"

	"github.com/daftpunkwav/breakwater/internal/httpserver"
	"github.com/daftpunkwav/breakwater/internal/obs"
	"github.com/daftpunkwav/breakwater/internal/relay"
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
			var trail []relay.AttemptTrace
			errorCode := rejectCode
			if carrier != nil {
				cacheHit = carrier.CacheHit
				if carrier.Relay != nil {
					upstream = carrier.Relay.UpstreamID
					aborted = carrier.Relay.Aborted
					streamed = carrier.Relay.Streamed
					tokens = carrier.Consumed
					trail = carrier.Relay.Trail
					// A forward-stage failure refines the rejection code:
					// upstream passthroughs classify by status, gateway
					// envelopes and stream aborts by their code. An
					// aborted stream already committed a 200, so the
					// abort flag is what carries its failure code.
					if errorCode == "" && (carrier.Relay.Status < 200 || carrier.Relay.Status > 299 || carrier.Relay.Aborted) {
						errorCode = carrier.Relay.ErrorCode
					}
				}
			}
			status := tee.Status()
			if status == 0 {
				// No header ever reached the wire. On the inference chains the
				// recovery stage sits inside this one, so a handler panic is
				// rendered as a counted 500 and never lands here. The admin,
				// probe and metrics routes have no observation stage at all,
				// so this branch is only ever a client that walked away.
				// Either way the aggregation counts it as a disconnect,
				// never a failure.
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
					Attempts:  attemptsToObs(trail),
				})
			}
		})
	}
}

// attemptsToObs converts the relay's attempt trail into the access
// log's shape; the log schema stays independent of the relay's.
func attemptsToObs(trail []relay.AttemptTrace) []obs.AttemptTrace {
	if len(trail) == 0 {
		return nil
	}
	out := make([]obs.AttemptTrace, len(trail))
	for i, a := range trail {
		out[i] = obs.AttemptTrace{Upstream: a.Upstream, Credential: a.CredentialIndex, Status: a.Status}
	}
	return out
}
