/**
 * @file chain_composition_test
 * @description The Chain composition contract: the first listed stage
 * runs outermost, and degenerate chains pass the final handler
 * through.
 */
package pipeline

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestChainRunsFirstStageOutermost composes two recording stages and
// asserts the full wrap order: the first listed stage opens and closes
// the exchange, the last hugs the terminal handler.
func TestChainRunsFirstStageOutermost(t *testing.T) {
	t.Parallel()

	var order []string
	// marking returns a stage that records its entry and exit around
	// whatever it wraps.
	marking := func(name string) Middleware {
		return func(next http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				order = append(order, name+"-in")
				next.ServeHTTP(w, r)
				order = append(order, name+"-out")
			})
		}
	}

	handler := Chain(marking("outer"), marking("inner"))(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		order = append(order, "final")
	}))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil))

	want := []string{"outer-in", "inner-in", "final", "inner-out", "outer-out"}
	if len(order) != len(want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order = %v, want %v", order, want)
		}
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}

// TestChainWithoutStagesServesFinal: an empty chain is the identity.
func TestChainWithoutStagesServesFinal(t *testing.T) {
	t.Parallel()

	handler := Chain()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	}))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Code != http.StatusTeapot {
		t.Fatalf("status = %d, want 418", rec.Code)
	}
}

// TestChainSingleStage: one stage still wraps the terminal handler.
func TestChainSingleStage(t *testing.T) {
	t.Parallel()

	handler := Chain(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Wrapped", "1")
			next.ServeHTTP(w, r)
		})
	})(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/", nil))
	if rec.Header().Get("X-Wrapped") != "1" {
		t.Fatalf("terminal handler not wrapped by the single stage")
	}
}
