/**
 * @file middleware
 * @description The cache pipeline stage: exact-match replay around the
 * rest of the chain.
 *
 * Responsibilities:
 * - Serve deterministic, cache-eligible requests from the store; a hit
 *   is zero-cost for the tenant (the whole reservation refunds)
 * - Deduplicate concurrent cold non-streaming fetches through the
 *   singleflight: exactly one upstream fetch per key (invariant I2);
 *   waiters replay the holder's result, errors included, with no
 *   implicit retry
 * - Streaming requests never share a flight (frozen decision, spec
 *   §6.3): they fetch individually and try to write the cache after
 *   completion, last write wins
 * - Nothing else: eligibility rules and key derivation live beside
 *   this file; storage sits behind the Cache port; the completions
 *   handler inside the chain owns consumption accounting
 */
package cache

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/daftpunkwav/breakwater/internal/httpserver"
	"github.com/daftpunkwav/breakwater/internal/obs"
	"github.com/daftpunkwav/breakwater/internal/pipeline"
	"github.com/daftpunkwav/breakwater/internal/protocol"
	"github.com/daftpunkwav/breakwater/internal/relay"
)

// maxCacheableBytes bounds the response size retained for caching; a
// bigger reply still reaches the client but is never stored.
const maxCacheableBytes = 8 << 20

// relayCache marks cache-served responses for observation.
const relayCache = "cache"

// relaySharedFetch marks singleflight waiters that rode an existing
// fetch.
const relaySharedFetch = "shared-fetch"

// Middleware returns the cache stage over a store, a flight group and
// the base TTL applied by the store (with its jitter).
func Middleware(store Cache, flight *Flight, ttl time.Duration, metrics *obs.Metrics) pipeline.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			carrier := pipeline.CarrierFrom(r.Context())
			if carrier == nil {
				protocol.WriteError(w, http.StatusInternalServerError, "pipeline_misconfigured",
					"no request carrier assembled")
				return
			}
			if !pipeline.EnsureBody(w, r, carrier) {
				return
			}
			// Frozen scope: only canonical-wire requests are cached. A
			// translated format would need its response re-rendered on
			// replay; until that lands, translated requests bypass.
			if carrier.Format != protocol.FormatOpenAIChat || !Eligible(carrier.Chat) {
				next.ServeHTTP(w, r)
				return
			}
			key := KeyFor(carrier.Body)

			if entry, err := store.Get(r.Context(), key); err == nil {
				metrics.CacheHit()
				replay(w, entry)
				carrier.CacheHit = true
				carrier.Consumed = 0
				carrier.Relay = &relay.Result{Status: entry.Status, UpstreamID: relayCache}
				return
			}
			metrics.CacheMiss()

			if carrier.Chat.Stream {
				// Frozen boundary: streams fetch individually; the flight
				// group is reserved for non-streaming requests.
				tee := httpserver.NewBufferingTee(w, maxCacheableBytes)
				next.ServeHTTP(tee, r)
				metrics.CacheFetch(upstreamOf(carrier))
				if storeWorthy(tee) && !relayAborted(carrier) {
					_ = store.Set(r.Context(), key, capture(tee), ttl)
				}
				return
			}

			entry, fetchErr, owner := flight.Do(r.Context(), key, func(ctx context.Context) (Entry, error) {
				tee := httpserver.NewBufferingTee(w, maxCacheableBytes)
				next.ServeHTTP(tee, r.WithContext(ctx))
				metrics.CacheFetch(upstreamOf(carrier))
				// A fetch whose handler produced no HTTP response at all
				// (its own client walked away before the first byte) has
				// nothing shareable: publishing the empty capture would
				// make waiters replay a header-less entry.
				if status := tee.Status(); status < 100 || status > 599 {
					return Entry{}, fmt.Errorf("cache: shared fetch produced no response")
				}
				return capture(tee), nil
			})
			if fetchErr != nil {
				// A waiter whose own context ended has a client gone: the
				// response would be noise. Any other waiter deserves a
				// real envelope instead of the collapsed shared fetch.
				if r.Context().Err() == nil {
					protocol.WireFor(carrier.Format).RenderError(w, http.StatusBadGateway,
						"upstream_unreachable", "the shared fetch for this request failed")
				}
				return
			}
			if owner {
				// The owner's response was already written through the
				// tee, and the completions handler already accounted its
				// consumption. Only storage remains.
				if storeWorthyFromEntry(entry) {
					_ = store.Set(r.Context(), key, entry, ttl)
				}
				return
			}
			// Waiter: the shared fetch's result is replayed verbatim;
			// it caused no upstream fetch of its own.
			metrics.CacheShared()
			replay(w, entry)
			carrier.CacheHit = true
			carrier.Consumed = 0
			carrier.Relay = &relay.Result{Status: entry.Status, UpstreamID: relaySharedFetch}
		})
	}
}

// replay writes a stored entry to a client.
func replay(w http.ResponseWriter, entry Entry) {
	header := w.Header()
	if ct := entry.Header.Get("Content-Type"); ct != "" {
		header.Set("Content-Type", ct)
	}
	w.WriteHeader(entry.Status)
	_, _ = w.Write(entry.Body)
	if flusher, ok := w.(http.Flusher); ok {
		flusher.Flush()
	}
}

// capture extracts the stored-entry form of what the handler wrote.
func capture(tee *httpserver.TeeResponseWriter) Entry {
	return Entry{
		Status: tee.Status(),
		Header: tee.Header().Clone(),
		Body:   tee.Body(),
	}
}

// upstreamOf names the upstream that served the request, as reported
// by the forward stage through the carrier.
func upstreamOf(carrier *pipeline.Carrier) string {
	if carrier.Relay != nil {
		return carrier.Relay.UpstreamID
	}
	return "-"
}

// storeWorthy reports whether a completed response should be stored:
// a 2xx that fit the capture buffer entirely.
func storeWorthy(tee *httpserver.TeeResponseWriter) bool {
	return tee.Status() >= 200 && tee.Status() < 300 && !tee.Truncated()
}

// relayAborted reports a stream the forward stage terminated through the
// error event contract: its bytes are a partial reply plus the error
// frame — never a replayable completion, whatever the HTTP status says.
func relayAborted(carrier *pipeline.Carrier) bool {
	return carrier.Relay != nil && carrier.Relay.Aborted
}

// storeWorthyFromEntry applies the same rule to a fetched entry.
func storeWorthyFromEntry(entry Entry) bool {
	return entry.Status >= 200 && entry.Status < 300 && len(entry.Body) <= maxCacheableBytes
}
