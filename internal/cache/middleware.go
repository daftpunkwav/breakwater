/**
 * @file middleware
 * @description The cache pipeline stage: exact-match replay around the
 * rest of the chain.
 *
 * Responsibilities:
 * - Serve deterministic, cache-eligible requests from the store. A
 *   hit sets carrier.Consumed to 0, which is what makes the
 *   surrounding stages refund in full: the limiter returns the whole
 *   reservation and the quota stage cancels the lease instead of
 *   settling it. This stage performs no refund of its own.
 * - Deduplicate concurrent cold non-streaming fetches through the
 *   singleflight: exactly one upstream fetch per key;
 *   waiters replay the holder's result, errors included, with no
 *   implicit retry
 * - Streaming requests never share a flight: they fetch individually
 *   and try to write the cache after
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
)

// maxCacheableBytes bounds the response size retained for caching; a
// bigger reply still reaches the client but is never stored.
const maxCacheableBytes = 8 << 20

// Middleware returns the cache stage over a store, a flight group and
// the base TTL applied by the store (with its jitter). fetchBudget
// bounds the shared fetch, which is not owned by the request that
// happens to start it: it must outlive that client's cancellation, so
// it needs a limit of its own. It should be the longest one request may
// run, since no request can legitimately consume upstream beyond that.
func Middleware(store Cache, flight *Flight, ttl time.Duration, metrics *obs.Metrics, fetchBudget time.Duration) pipeline.Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			carrier, ok := pipeline.RequireCarrier(w, r)
			if !ok {
				return
			}
			if !pipeline.EnsureBody(w, r, carrier) {
				return
			}
			// Scope: only canonical-wire requests are cached. A
			// translated format would need its stored response
			// re-rendered into the client's shape on replay, which
			// this stage does not do; translated requests bypass.
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
				// The replay origin rides ServedBy, not the relay
				// result: no upstream served this response, and the
				// forward stage's outcome must not say otherwise.
				carrier.ServedBy = pipeline.SourceCache
				return
			}
			metrics.CacheMiss()

			if carrier.Chat.Stream {
				// Scope: streams fetch individually; the flight
				// group is reserved for non-streaming requests.
				tee := httpserver.NewBufferingTee(w, maxCacheableBytes)
				next.ServeHTTP(tee, r)
				metrics.CacheFetch(upstreamOf(carrier))
				if storeWorthy(tee) && !relayAborted(carrier) {
					_ = store.Set(r.Context(), key, capture(tee), ttl)
				}
				return
			}

			// The shared fetch is not owned by the request that happened
			// to start it. Its context is detached from that client's
			// cancellation and its tee tolerates that client's writes
			// failing, so a starter that walks away mid-flight neither
			// cancels the upstream call nor fails every request waiting
			// on the same key. Detaching also drops the deadline the
			// request carried, so the budget is re-imposed here: an
			// unbounded fetch would leave the key unable to start a new
			// flight until the process restarts.
			fetchCtx, cancelFetch := context.WithTimeout(context.WithoutCancel(r.Context()), fetchBudget)
			defer cancelFetch()

			fetch := httpserver.NewDetachedBufferingTee(w, maxCacheableBytes)
			entry, fetchErr, owner := flight.Do(fetchCtx, key, func(ctx context.Context) (Entry, error) {
				// The budget rides the fetch itself, not the retry policy
				// that may contribute none: WithoutCancel drops the
				// deadline fetchCtx carries, so it is re-imposed here. A
				// black-holed upstream must release the flight at the
				// budget, or the key can never start a new flight.
				bounded, cancelBounded := context.WithTimeout(context.WithoutCancel(ctx), fetchBudget)
				defer cancelBounded()
				next.ServeHTTP(fetch, r.WithContext(bounded))
				metrics.CacheFetch(upstreamOf(carrier))
				// A fetch whose handler produced no HTTP response at all
				// (its own client walked away before the first byte) has
				// nothing shareable: publishing the empty capture would
				// make waiters replay a header-less entry. A capture cut
				// off at the byte cap is equally unshareable: waiters
				// would replay a truncated body as a complete reply.
				if status := fetch.Status(); status < 100 || status > 599 {
					return Entry{}, fmt.Errorf("cache: shared fetch produced no response")
				}
				if fetch.Truncated() {
					return Entry{}, fmt.Errorf("cache: shared fetch exceeded the shareable size")
				}
				return capture(fetch), nil
			})
			if fetchErr != nil {
				// A starter whose tee already put a response on the wire
				// owes its client nothing more — appending an envelope
				// there would corrupt the reply it is reading. A
				// response-less fetch, and every waiter, still owes one.
				if (!owner || fetch.Status() < 100) && r.Context().Err() == nil {
					protocol.WireFor(carrier.Format).RenderError(w, http.StatusBadGateway,
						"upstream_unreachable", "the shared fetch for this request failed")
				}
				return
			}
			if !owner {
				// Waiter: the shared fetch's result is replayed verbatim;
				// it caused no upstream fetch of its own, so its
				// reservation refunds in full.
				metrics.CacheShared()
				replay(w, entry)
				carrier.CacheHit = true
				carrier.Consumed = 0
				carrier.ServedBy = pipeline.SourceSharedFetch
				return
			}
			// Starter: the response already went to its own connection
			// through the tee, and the chain already accounted its real
			// consumption. Only storage remains: a full entry for the
			// base TTL, an upstream failure or empty success for a short
			// negative one. The write uses the fetch's context, not the
			// starter's: a starter that left mid-flight must not cost the
			// waiters — or every later request — the warmed entry.
			switch {
			case storeWorthyFromEntry(entry):
				_ = store.Set(fetchCtx, key, entry, ttl)
			case negativelyCacheable(entry, carrier):
				_ = store.Set(fetchCtx, key, entry, ttl/10)
			}
		})
	}
}

// replay forwards a stored entry's headers through the relay's single
// passthrough set (protocol.PassthroughHeaderNames): the body's media
// type, and the Retry-After a negatively cached 429/5xx owes its
// client — a hit that drops it invites the immediate retry the upstream
// asked to wait out. One list, two consumers: neither can drift. The
// values need no legality re-check here: the only writers store headers
// already validated by the passthrough surfaces at capture time.
func replay(w http.ResponseWriter, entry Entry) {
	header := w.Header()
	for _, name := range protocol.PassthroughHeaderNames() {
		if v := entry.Header.Get(name); v != "" {
			header.Set(name, v)
		}
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

// negativelyCacheable reports whether a failed exchange is a fact about
// the request worth remembering briefly:
// an error the upstream itself produced, or an empty success. Gateway
// envelopes (circuit open, budget exhausted, unreachable) are transient
// gateway states, never facts, and never qualify. A response origin
// that is not an upstream (a cache replay, a shared fetch), and a
// forward that never named an upstream, are equally not facts.
func negativelyCacheable(entry Entry, carrier *pipeline.Carrier) bool {
	relayResult := carrier.Relay
	if carrier.ServedBy != "" || relayResult == nil || relayResult.UpstreamID == "" {
		return false
	}
	switch {
	case entry.Status >= http.StatusBadRequest && entry.Status <= http.StatusInsufficientStorage:
		return true
	case entry.Status >= 200 && entry.Status < 300:
		return len(entry.Body) == 0
	}
	return false
}

// storeWorthyFromEntry applies the same rule to a fetched entry.
func storeWorthyFromEntry(entry Entry) bool {
	return entry.Status >= 200 && entry.Status < 300 && len(entry.Body) <= maxCacheableBytes
}
