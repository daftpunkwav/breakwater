/**
 * @file middleware_test
 * @description Cache stage integration: the full-stack I2 evidence
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
		_, _ = w.Write([]byte(fmt.Sprintf(`{"choices":[],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":%d}}`, fetches.Load())))
	})
}

// cacheStage builds the cache stage applied over the given upstream,
// with the carrier stage the pipeline always runs first.
func cacheStage(t *testing.T, upstream http.Handler) http.Handler {
	t.Helper()
	return pipeline.Chain(
		pipeline.CarrierStage(),
		Middleware(NewMemory(), NewFlight(), time.Minute),
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

// TestCacheMiddlewareConcurrentColdStartsFetchOnce is the I2 evidence:
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
			<-start
			codes <- fireRequest(handler, body).Code
		}()
	}
	close(start)
	wg.Wait()
	close(codes)

	if got := fetches.Load(); got != 1 {
		t.Fatalf("fetches = %d, want exactly 1 (I2)", got)
	}
	for code := range codes {
		if code != http.StatusOK {
			t.Fatalf("caller status = %d, want 200", code)
		}
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
