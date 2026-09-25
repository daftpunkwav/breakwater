/**
 * @file handler_routing_test
 * @description The HTTP surface guards of the mock upstream: route
 * table (health, completions, unknown paths), method guards, strict
 * directive rejection, body validation and the size cap.
 */
package mockllm

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// postCompletion fires one POST against the handler directly and
// returns the recorder.
func postCompletion(t *testing.T, h *Handler, headers map[string]string, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestRouting(t *testing.T) {
	t.Parallel()
	h := New(Options{})

	// Unknown paths 404.
	req := httptest.NewRequest(http.MethodGet, "/nope", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("unknown path status = %d, want 404", rec.Code)
	}

	// Completions require POST.
	req = httptest.NewRequest(http.MethodGet, "/v1/chat/completions", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET completions status = %d, want 405", rec.Code)
	}

	// Malformed JSON bodies 400.
	rec = postCompletion(t, h, nil, `{not json}`)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("malformed body status = %d, want 400", rec.Code)
	}

	// Missing model or messages 400.
	rec = postCompletion(t, h, nil, `{"model":"","messages":[]}`)
	if rec.Code != http.StatusBadRequest {
		t.Errorf("missing model status = %d, want 400", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "model and messages are required") {
		t.Errorf("body = %s, want the validation message", rec.Body.String())
	}
}

// TestHealthProbe pins the probe contract: GET answers ok, every other
// method is a 405.
func TestHealthProbe(t *testing.T) {
	t.Parallel()
	h := New(Options{})

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.HasPrefix(rec.Body.String(), "ok") {
		t.Fatalf("GET /healthz = %d %q, want 200 ok", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodPost, "/healthz", nil)
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST /healthz = %d, want 405", rec.Code)
	}
}

// TestStrictFaultDirectiveRejected pins that a malformed directive is a
// loud 400 (invalid_fault_directive), never silently ignored.
func TestStrictFaultDirectiveRejected(t *testing.T) {
	t.Parallel()
	h := New(Options{})

	rec := postCompletion(t, h, map[string]string{headerStreamMode: "explode"},
		`{"model":"mock-gpt","messages":[{"role":"user","content":"hi"}]}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "invalid_fault_directive") {
		t.Fatalf("body = %s, want the directive envelope", rec.Body.String())
	}
}

func TestBodyTooLarge(t *testing.T) {
	t.Parallel()
	h := New(Options{})

	huge := strings.Repeat("a", maxRequestBytes+1024)
	rec := postCompletion(t, h, nil,
		`{"model":"mock-gpt","messages":[{"role":"user","content":"`+huge+`"}]}`)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", rec.Code)
	}
}
