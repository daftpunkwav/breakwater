/**
 * @file requestid_test
 * @description Request-ID stage tests: adopt-or-mint policy, the echo
 * header on every outcome, carrier attachment and the validity rule
 * for client-supplied ids.
 */
package pipeline

import (
	"context"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"
)

// requestIdHandler mirrors the production wiring: the carrier stage
// runs first, the request-ID stage second.
func requestIdHandler() http.Handler {
	return Chain(
		CarrierStage(),
		RequestIDStage(),
	)(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot) // any status; the header must exist regardless
	}))
}

func TestRequestIDMintedWhenAbsent(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	requestIdHandler().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil))

	id := rec.Header().Get(RequestIDHeader)
	if !regexp.MustCompile(`^req-[0-9a-f]{24}$`).MatchString(id) {
		t.Fatalf("minted id = %q, want the req- hex form", id)
	}
}

func TestRequestIDAdoptedWhenWellFormed(t *testing.T) {
	t.Parallel()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	req.Header.Set(RequestIDHeader, "my-trace-1234")
	rec := httptest.NewRecorder()
	requestIdHandler().ServeHTTP(rec, req)

	if got := rec.Header().Get(RequestIDHeader); got != "my-trace-1234" {
		t.Fatalf("adopted id = %q, want the client value verbatim", got)
	}
}

func TestRequestIDReplacesMalformedCandidates(t *testing.T) {
	t.Parallel()
	for _, bad := range []string{
		"",
		"short",
		"has space",
		"with\nnewline",
		strings.Repeat("x", 129),
	} {
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
		req.Header.Set(RequestIDHeader, bad)
		rec := httptest.NewRecorder()
		requestIdHandler().ServeHTTP(rec, req)

		id := rec.Header().Get(RequestIDHeader)
		if id == bad {
			t.Errorf("malformed id %q must be replaced", bad)
		}
		if !strings.HasPrefix(id, "req-") {
			t.Errorf("replacement %q not gateway-minted", id)
		}
	}
}

func TestRequestIDAttachedToCarrier(t *testing.T) {
	t.Parallel()
	var seen string
	handler := Chain(
		CarrierStage(),
		RequestIDStage(),
	)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		carrier := CarrierFrom(r.Context())
		if carrier == nil {
			t.Error("no carrier on the context")
			return
		}
		seen = carrier.RequestID
	}))
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	req.Header.Set(RequestIDHeader, "client-id-99")
	handler.ServeHTTP(httptest.NewRecorder(), req)

	if seen != "client-id-99" {
		t.Fatalf("carrier id = %q, want the adopted value", seen)
	}
}

func TestRequestIDContextHelpers(t *testing.T) {
	t.Parallel()
	if CarrierFrom(context.Background()) != nil {
		t.Fatal("empty context must carry no carrier")
	}
}
