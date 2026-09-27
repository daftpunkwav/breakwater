/**
 * @file recovery_test
 * @description The panic containment stage: a panic from the chain's
 * end renders a counted 500 before the response starts, and stays a
 * logged truncation once headers are committed.
 */
package pipeline

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/daftpunkwav/breakwater/internal/protocol"
)

func recoveryRequest(t *testing.T, handler http.Handler) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader([]byte(`{}`)))
	req = req.WithContext(WithCarrier(req.Context(), &Carrier{Format: protocol.FormatOpenAIChat}))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func TestRecoveryStageRendersPanicAs500(t *testing.T) {
	t.Parallel()
	panicHandler := RecoveryStage()(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("upstream handler exploded")
	}))
	rec := recoveryRequest(t, panicHandler)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500: a panic is a gateway fault, not a killed connection", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "internal_error") {
		t.Fatalf("body = %s, want the internal_error envelope", rec.Body.String())
	}
}

func TestRecoveryStageAbsorbsCleanHandlers(t *testing.T) {
	t.Parallel()
	healthy := RecoveryStage()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	rec := recoveryRequest(t, healthy)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 through the stage", rec.Code)
	}
}

func TestRecoveryStageNeverWritesAfterCommit(t *testing.T) {
	t.Parallel()
	// A handler that committed a response and then panics: the stage
	// must only log — a second WriteHeader would panic the writer in
	// turn.
	committed := RecoveryStage()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("partial"))
		panic("mid-stream")
	}))
	rec := recoveryRequest(t, committed)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want the committed 200 untouched", rec.Code)
	}
	if !strings.HasPrefix(rec.Body.String(), "partial") {
		t.Fatalf("body = %q, want the bytes written before the panic", rec.Body.String())
	}
}
