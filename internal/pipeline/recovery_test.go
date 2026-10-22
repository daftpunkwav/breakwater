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

// recoveryReq builds the carrier-carrying request the recovery tests
// serve, without binding it to a recorder.
func recoveryReq(t *testing.T) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader([]byte(`{}`)))
	return req.WithContext(WithCarrier(req.Context(), &Carrier{Format: protocol.FormatOpenAIChat}))
}

func recoveryRequest(t *testing.T, handler http.Handler) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, recoveryReq(t))
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

// statusWriter is a writer that reports whether its headers are
// committed — the shape the production chain hands the recovery stage
// (the tee response writer implements Status; a bare recorder does
// not).
type statusWriter struct {
	header http.Header
	code   int
	body   strings.Builder
}

func newStatusWriter() *statusWriter { return &statusWriter{header: make(http.Header)} }

func (w *statusWriter) Header() http.Header         { return w.header }
func (w *statusWriter) WriteHeader(code int)        { w.code = code }
func (w *statusWriter) Write(p []byte) (int, error) { return w.body.Write(p) }
func (w *statusWriter) Status() int                 { return w.code }

// TestRecoveryStageNeverWritesAfterCommit: a writer that can report a
// committed response makes the stage log-only. A second envelope after
// the commit would corrupt the started body on the wire — the exact
// failure the commit check in the recovery path exists to prevent.
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
	w := newStatusWriter()
	committed.ServeHTTP(w, recoveryReq(t))
	if w.code != http.StatusOK {
		t.Fatalf("status = %d, want the committed 200 untouched", w.code)
	}
	if got := w.body.String(); got != "partial" {
		t.Fatalf("body = %q, want exactly the bytes written before the panic (a late envelope must not be appended)", got)
	}
}

// TestRecoveryStageRendersEnvelopeWhenTheWriterCannotReportCommit: a
// writer with no way to report its commit state gets the 500 envelope
// appended anyway — best effort; the transport drops or garbles what it
// must. The stage must never skip the error render just because the
// writer stays silent about its state.
func TestRecoveryStageRendersEnvelopeWhenTheWriterCannotReportCommit(t *testing.T) {
	t.Parallel()
	committed := RecoveryStage()(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("partial"))
		panic("mid-stream")
	}))
	rec := recoveryRequest(t, committed)
	if !strings.HasPrefix(rec.Body.String(), "partial") {
		t.Fatalf("body = %q, want the bytes written before the panic", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "internal_error") {
		t.Fatalf("body = %q, want the error envelope appended for a commit-blind writer", rec.Body.String())
	}
}
