/**
 * @file concurrency_middleware_test
 * @description The concurrency pipeline stage: the ceiling rejects
 * with 429 concurrency_limit_exceeded before the terminal stage, the
 * slot is released when the handler returns, and a disabled ceiling
 * lets everything through.
 */
package limiter

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/daftpunkwav/breakwater/internal/auth"
	"github.com/daftpunkwav/breakwater/internal/obs"
	"github.com/daftpunkwav/breakwater/internal/pipeline"
	"github.com/daftpunkwav/breakwater/internal/protocol"
)

// serveThroughConcurrency runs one carrier-backed request through the
// stage and returns the recorder; the terminal handler answers
// immediately, so each call takes and releases its slot synchronously.
func serveThroughConcurrency(t *testing.T, g *Concurrency, tenant auth.Tenant) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(threeWordBody))
	carrier := &pipeline.Carrier{Tenant: tenant, Format: protocol.FormatOpenAIChat}
	req = req.WithContext(pipeline.WithCarrier(req.Context(), carrier))

	terminal := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	rec := httptest.NewRecorder()
	ConcurrencyMiddleware(g, obs.NewMetrics())(terminal).ServeHTTP(rec, req)
	return rec
}

func TestConcurrencyMiddlewareRejectsOverCeiling(t *testing.T) {
	t.Parallel()
	g := NewConcurrency()
	blocked := auth.Tenant{ID: "t1", Tier: auth.Tier{Concurrency: 1}}

	// Occupy the one slot directly.
	release, ok := g.Acquire("t1", 1)
	if !ok {
		t.Fatal("first acquire failed")
	}
	defer release()

	rec := serveThroughConcurrency(t, g, blocked)
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "concurrency_limit_exceeded") {
		t.Fatalf("body = %s, want the concurrency envelope", rec.Body.String())
	}
	// The same 429 header discipline as the rate-limit stage: the
	// rejection carries Retry-After guidance.
	if after := rec.Header().Get("Retry-After"); after != "1" {
		t.Fatalf("Retry-After = %q, want \"1\"", after)
	}
}

func TestConcurrencyMiddlewareAdmitsUnderCeilingAndReleases(t *testing.T) {
	t.Parallel()
	g := NewConcurrency()
	tenant := auth.Tenant{ID: "t1", Tier: auth.Tier{Concurrency: 2}}

	for i := 0; i < 5; i++ {
		rec := serveThroughConcurrency(t, g, tenant)
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d status = %d, want 200 (each request releases before returning)", i+1, rec.Code)
		}
	}
	if g.InFlight("t1") != 0 {
		t.Fatalf("in-flight = %d, want 0: the slot must ride the handler return", g.InFlight("t1"))
	}
}

func TestConcurrencyMiddlewareWithoutCarrier(t *testing.T) {
	t.Parallel()
	g := NewConcurrency()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(threeWordBody))
	rec := httptest.NewRecorder()

	called := false
	ConcurrencyMiddleware(g, nil)(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		called = true
	})).ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError || called {
		t.Fatalf("status = %d called = %v, want the misconfigured rejection", rec.Code, called)
	}
}
