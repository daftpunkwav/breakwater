/**
 * @file middleware_test
 * @description Cache stage integration: the full-stack single-fetch evidence
 * (concurrent cold starts fetch upstream exactly once), hit refunds,
 * the eligibility bypass and the streaming boundary.
 */
package cache

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/daftpunkwav/breakwater/internal/pipeline"
	"github.com/daftpunkwav/breakwater/internal/protocol"
	"github.com/daftpunkwav/breakwater/internal/relay"
)

// countingUpstream answers chat completions and counts fetches; stream
// requests get a minimal SSE body. The body comes from the carrier —
// the cache stage already consumed r.Body.
func countingUpstream(t *testing.T, fetches *atomic.Int64, holdStart <-chan struct{}) http.Handler {
	t.Helper()
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw := string(pipeline.CarrierFrom(r.Context()).Body)
		if strings.Contains(raw, `"stream":true`) {
			<-holdStart
			fetches.Add(1)
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			flusher := w.(http.Flusher)
			for _, chunk := range []string{
				`data: {"choices":[{"delta":{"content":"hi"}}]}` + "\n\n",
				"data: [DONE]\n\n",
			} {
				if _, err := w.Write([]byte(chunk)); err != nil {
					return
				}
				flusher.Flush()
			}
			return
		}
		<-holdStart
		fetches.Add(1)
		w.Header().Set("Content-Type", "application/json")
		// A broken client write ends the exchange; nothing to observe.
		_, _ = fmt.Fprintf(w, `{"choices":[],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":%d}}`, fetches.Load())
	})
}

// cacheStage builds the cache stage applied over the given upstream,
// with the carrier and format stages the pipeline always runs first.
func cacheStage(t *testing.T, upstream http.Handler) http.Handler {
	t.Helper()
	return pipeline.Chain(
		pipeline.CarrierStage(),
		pipeline.FormatStage(protocol.FormatOpenAIChat),
		Middleware(NewMemory(), NewFlight(), time.Minute, nil, time.Minute),
	)(upstream)
}

func fireRequest(handler http.Handler, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func TestCacheMiddlewareHitRefetchesNothing(t *testing.T) {
	t.Parallel()
	var fetches atomic.Int64
	handler := cacheStage(t, countingUpstream(t, &fetches, blockingOpen()))

	body := `{"model":"m","temperature":0,"messages":[{"role":"user","content":"hi"}]}`
	first := fireRequest(handler, body)
	if first.Code != http.StatusOK {
		t.Fatalf("first status = %d", first.Code)
	}
	second := fireRequest(handler, body)
	if second.Code != http.StatusOK || second.Body.String() != first.Body.String() {
		t.Fatalf("replay mismatch: %d %q vs %q", second.Code, second.Body.String(), first.Body.String())
	}
	if got := fetches.Load(); got != 1 {
		t.Fatalf("fetches = %d, want 1 (second served from cache)", got)
	}
}

func blockingOpen() <-chan struct{} {
	h := make(chan struct{})
	close(h)
	return h
}

// TestCacheMiddlewareConcurrentColdStartsFetchOnce is the single-fetch
// evidence:
// a stampede of identical cold requests must produce one upstream
// fetch, and every caller must receive the response.
func TestCacheMiddlewareConcurrentColdStartsFetchOnce(t *testing.T) {
	t.Parallel()
	var fetches atomic.Int64
	start := make(chan struct{})
	handler := cacheStage(t, countingUpstream(t, &fetches, start))

	const callers = 32
	body := `{"model":"m","temperature":0,"messages":[{"role":"user","content":"hi"}]}`

	var wg sync.WaitGroup
	codes := make(chan int, callers)
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			codes <- fireRequest(handler, body).Code
		}()
	}
	// Release the (slow) fetch only after every caller had the chance
	// to pile onto the flight; otherwise fast owners finish alone.
	go func() {
		time.Sleep(100 * time.Millisecond)
		close(start)
	}()
	wg.Wait()
	close(codes)

	if got := fetches.Load(); got != 1 {
		t.Fatalf("fetches = %d, want exactly 1", got)
	}
	for code := range codes {
		if code != http.StatusOK {
			t.Fatalf("caller status = %d, want 200", code)
		}
	}
}

// TestCacheMiddlewareNegativeCachesUpstreamErrors pins the
// penetration guard: an upstream-produced error is stored for a short
// TTL, so a flood of identical bad requests stops at the cache.
func TestCacheMiddlewareNegativeCachesUpstreamErrors(t *testing.T) {
	t.Parallel()
	var fetches atomic.Int64
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fetches.Add(1)
		// Mirror the inference handler's contract: the relay result is
		// on the carrier by the time the cache stage stores.
		pipeline.CarrierFrom(r.Context()).Relay = &relay.Result{
			Status:     http.StatusNotFound,
			UpstreamID: "u1",
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":{"message":"no such model"}}`))
	})
	handler := cacheStage(t, upstream)

	body := `{"model":"ghost","temperature":0,"messages":[{"role":"user","content":"hi"}]}`
	if rec := fireRequest(handler, body); rec.Code != http.StatusNotFound {
		t.Fatalf("first status = %d, want 404", rec.Code)
	}
	second := fireRequest(handler, body)
	if second.Code != http.StatusNotFound {
		t.Fatalf("second status = %d, want the replayed 404", second.Code)
	}
	if got := fetches.Load(); got != 1 {
		t.Fatalf("fetches = %d, want 1 (negative entry served the second)", got)
	}
}

// TestCacheMiddlewareReplayCarriesRetryAfter pins the negative-cache
// replay contract: the Retry-After a stored 429 carries is part of the
// fact being cached, and a hit that drops it invites the immediate
// retry the upstream asked to wait out.
func TestCacheMiddlewareReplayCarriesRetryAfter(t *testing.T) {
	t.Parallel()
	var fetches atomic.Int64
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fetches.Add(1)
		pipeline.CarrierFrom(r.Context()).Relay = &relay.Result{
			Status:     http.StatusTooManyRequests,
			UpstreamID: "u1",
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"error":{"message":"slow down"}}`))
	})
	handler := cacheStage(t, upstream)

	body := `{"model":"m","temperature":0,"messages":[{"role":"user","content":"hi"}]}`
	if rec := fireRequest(handler, body); rec.Code != http.StatusTooManyRequests {
		t.Fatalf("first status = %d, want 429", rec.Code)
	}
	second := fireRequest(handler, body)
	if second.Code != http.StatusTooManyRequests {
		t.Fatalf("second status = %d, want the replayed 429", second.Code)
	}
	if got := second.Header().Get("Retry-After"); got != "7" {
		t.Fatalf("replayed Retry-After = %q, want the stored hint", got)
	}
	if got := fetches.Load(); got != 1 {
		t.Fatalf("fetches = %d, want 1 (negative entry served the second)", got)
	}
}

// TestCacheMiddlewareNeverNegativeCachesGatewayEnvelopes pins the other
// half: a gateway envelope is a transient state, not a fact about the
// request — every request must reach the upstream again.
func TestCacheMiddlewareNeverNegativeCachesGatewayEnvelopes(t *testing.T) {
	t.Parallel()
	var fetches atomic.Int64
	// A handler that dies before its first byte produces a response-less
	// fetch: no status, nothing shareable, nothing cacheable.
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fetches.Add(1)
		_ = r // never write anything
	})
	handler := cacheStage(t, upstream)

	body := `{"model":"m","temperature":0,"messages":[{"role":"user","content":"hi"}]}`
	fireRequest(handler, body)
	fireRequest(handler, body)
	if got := fetches.Load(); got != 2 {
		t.Fatalf("fetches = %d, want 2: nothing response-less may be cached", got)
	}
}

func TestCacheMiddlewareIneligibleRequestsBypass(t *testing.T) {
	t.Parallel()
	var fetches atomic.Int64
	handler := cacheStage(t, countingUpstream(t, &fetches, blockingOpen()))

	// temperature > 0: never cached, never deduplicated.
	for range 3 {
		if rec := fireRequest(handler, `{"model":"m","temperature":0.7,"messages":[{"role":"user","content":"hi"}]}`); rec.Code != http.StatusOK {
			t.Fatalf("status = %d", rec.Code)
		}
	}
	if got := fetches.Load(); got != 3 {
		t.Fatalf("fetches = %d, want 3: sampling requests must bypass the cache", got)
	}
}

func TestCacheMiddlewareStreamBoundary(t *testing.T) {
	t.Parallel()
	var fetches atomic.Int64
	handler := cacheStage(t, countingUpstream(t, &fetches, blockingOpen()))

	body := `{"model":"m","stream":true,"temperature":0,"messages":[{"role":"user","content":"hi"}]}`
	first := fireRequest(handler, body)
	if first.Code != http.StatusOK || !strings.HasSuffix(first.Body.String(), "data: [DONE]\n\n") {
		t.Fatalf("first stream: %d %q", first.Code, first.Body.String())
	}
	// The completed stream response was cached: the second replay is a
	// stream too, without a new fetch.
	second := fireRequest(handler, body)
	if second.Code != http.StatusOK || !strings.Contains(second.Body.String(), `"content":"hi"`) {
		t.Fatalf("replayed stream: %d %q", second.Code, second.Body.String())
	}
	if got := fetches.Load(); got != 1 {
		t.Fatalf("fetches = %d, want 1: the replay must come from the cache", got)
	}
}

// oversizedUpstream answers with a body larger than the retention cap;
// the cache contract says it still reaches its own client in full, but
// the truncated capture must be neither stored nor shared.
func abortingUpstream(fetches *atomic.Int64) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fetches.Add(1)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("data: {\"choices\":[{\"delta\":{\"content\":\"par\"}}]}\n\n" +
			"event: error\ndata: {\"error\":{\"code\":\"upstream_reset\"}}\n\ndata: [DONE]\n\n"))
		if carrier := pipeline.CarrierFrom(r.Context()); carrier != nil {
			carrier.Relay = &relay.Result{Status: http.StatusOK, Streamed: true, Aborted: true}
		}
	})
}

// TestCacheMiddlewareNeverStoresAbortedStreams guards the honesty rule:
// a stream terminated through the error contract is a failure, and its
// bytes must never come back as a cached completion.
func TestCacheMiddlewareNeverStoresAbortedStreams(t *testing.T) {
	t.Parallel()
	var fetches atomic.Int64
	handler := pipeline.Chain(
		pipeline.CarrierStage(),
		pipeline.FormatStage(protocol.FormatOpenAIChat),
		Middleware(NewMemory(), NewFlight(), time.Minute, nil, time.Minute),
	)(abortingUpstream(&fetches))

	body := `{"model":"m","stream":true,"temperature":0,"messages":[{"role":"user","content":"hi"}]}`
	for i := range 2 {
		rec := fireRequest(handler, body)
		if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"code":"upstream_reset"`) {
			t.Fatalf("request %d: %d %q", i, rec.Code, rec.Body.String())
		}
	}
	if got := fetches.Load(); got != 2 {
		t.Fatalf("fetches = %d, want 2: the aborted stream must not be cached", got)
	}
}

// TestCacheMiddlewareWaiterSurvivesOwnerDisconnect locks the shared
// fetch failure contract: an owner whose client walks away mid-fetch
// produces no response at all; its empty capture must fail the flight
// instead of being replayed by waiters (a header-less WriteHeader with
func TestNegativelyCacheableRules(t *testing.T) {
	t.Parallel()
	upstream := &pipeline.Carrier{Relay: &relay.Result{UpstreamID: "u1"}}
	cases := []struct {
		name  string
		entry Entry
		relay *relay.Result
		want  bool
	}{
		{"upstream 4xx is a fact", Entry{Status: 404, Body: []byte(`{"error":"no"}`)}, &relay.Result{UpstreamID: "u1"}, true},
		{"upstream 5xx is a fact", Entry{Status: 503, Body: []byte(`{"error":"down"}`)}, &relay.Result{UpstreamID: "u1"}, true},
		{"empty 200 is a fact", Entry{Status: 200}, &relay.Result{UpstreamID: "u1"}, true},
		{"non-empty 200 is not", Entry{Status: 200, Body: []byte(`{"a":1}`)}, &relay.Result{UpstreamID: "u1"}, false},
		{"3xx is neither", Entry{Status: 302, Body: []byte("x")}, &relay.Result{UpstreamID: "u1"}, false},
		{"no relay result", Entry{Status: 404}, nil, false},
		{"cache replay is not a fact", Entry{Status: 404}, &relay.Result{UpstreamID: observedUpstreamCache}, false},
		{"shared fetch is not a fact", Entry{Status: 404}, &relay.Result{UpstreamID: observedUpstreamSharedFetch}, false},
		{"upstream unknown is not a fact", Entry{Status: 404}, &relay.Result{}, false},
	}
	for _, tc := range cases {
		carrier := &pipeline.Carrier{Relay: tc.relay}
		if got := negativelyCacheable(tc.entry, carrier); got != tc.want {
			t.Errorf("%s: negativelyCacheable = %v, want %v", tc.name, got, tc.want)
		}
	}
	if negativelyCacheable(Entry{Status: 404}, upstream) != true {
		t.Error("sanity: the upstream-carrying carrier must qualify")
	}
}

// ctxHonoringStore is a Cache that behaves the way the port's context
// parameter implies: a write on a finished context fails. The shipped
// in-memory store ignores its context entirely, which is why riding the
// starter's cancelled request looked harmless — it is a property of
// that one implementation, not of the contract.
