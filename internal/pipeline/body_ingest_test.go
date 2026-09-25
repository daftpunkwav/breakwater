/**
 * @file body_ingest_test
 * @description The single body read: EnsureBody's full path set (nil
 * carrier, already-read, oversized, unreadable, malformed, and the two
 * successful ingests) plus the format pinning stage.
 */
package pipeline

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/daftpunkwav/breakwater/internal/protocol"
)

// errReader is a request body that fails mid-read: the transport-level
// unreadable-body shape.
type errReader struct{ err error }

// Read implements io.Reader.
func (r errReader) Read([]byte) (int, error) { return 0, r.err }

// runEnsureBody serves one request through EnsureBody and returns the
// recorder and the outcome.
func runEnsureBody(t *testing.T, format protocol.Format, body io.Reader, preset *Carrier) (*httptest.ResponseRecorder, *Carrier, bool) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", body)
	carrier := preset
	if carrier == nil {
		carrier = &Carrier{Format: format}
	}
	rec := httptest.NewRecorder()
	ok := EnsureBody(rec, req, carrier)
	return rec, carrier, ok
}

// TestEnsureBodyWithoutCarrier: the chain-entry guard fires before any
// read.
func TestEnsureBodyWithoutCarrier(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader("{}"))
	rec := httptest.NewRecorder()

	if ok := EnsureBody(rec, req, nil); ok {
		t.Fatal("EnsureBody = true, want false without a carrier")
	}
	assertEnvelope(t, rec, http.StatusInternalServerError, "pipeline_misconfigured")
}

// TestEnsureBodyRerendersStoredIngestError: a carrier that already
// carries a failed read renders 400 in its format without re-reading.
func TestEnsureBodyRerendersStoredIngestError(t *testing.T) {
	t.Parallel()

	preset := &Carrier{Format: protocol.FormatOpenAIChat, ChatSet: true,
		ChatErr: errors.New("model is required")}
	rec, carrier, ok := runEnsureBody(t, "", nil, preset)

	if ok {
		t.Fatal("EnsureBody = true, want false for a stored ingest error")
	}
	assertEnvelope(t, rec, http.StatusBadRequest, "invalid_request")
	if !strings.Contains(rec.Body.String(), "model is required") {
		t.Fatalf("body does not carry the stored error: %s", rec.Body.String())
	}
	if carrier.Chat.Model != "" {
		t.Fatal("the stored state must not be replaced")
	}
}

// TestEnsureBodyAlreadyReadPassesThrough: a successfully read carrier
// short-circuits to true and writes nothing.
func TestEnsureBodyAlreadyReadPassesThrough(t *testing.T) {
	t.Parallel()

	preset := &Carrier{Format: protocol.FormatOpenAIChat, ChatSet: true,
		Chat: protocol.ChatRequest{Model: "m1"}, Body: []byte(`{"model":"m1"}`)}
	rec, carrier, ok := runEnsureBody(t, "", nil, preset)

	if !ok {
		t.Fatal("EnsureBody = false, want true for an already-read carrier")
	}
	if rec.Code != http.StatusOK || rec.Body.Len() != 0 {
		t.Fatalf("a pass-through must write nothing, got status %d body %q", rec.Code, rec.Body.String())
	}
	if carrier.Chat.Model != "m1" {
		t.Fatalf("canonical chat was touched: %+v", carrier.Chat)
	}
}

// TestEnsureBodyIngestsOpenAIChat: the canonical wire stores the raw
// body verbatim as the upstream body.
func TestEnsureBodyIngestsOpenAIChat(t *testing.T) {
	t.Parallel()

	raw := `{"model":"m1","messages":[{"role":"user","content":"hi"}]}`
	rec, carrier, ok := runEnsureBody(t, protocol.FormatOpenAIChat, strings.NewReader(raw), nil)

	if !ok {
		t.Fatalf("EnsureBody = false, body: %s", rec.Body.String())
	}
	if string(carrier.Body) != raw {
		t.Fatalf("carrier.Body = %q, want the raw request", carrier.Body)
	}
	if carrier.Chat.Model != "m1" || len(carrier.Chat.Messages) != 1 {
		t.Fatalf("canonical chat = %+v, want model m1 with one message", carrier.Chat)
	}
	if string(carrier.UpstreamBody) != raw {
		t.Fatalf("UpstreamBody = %q, want the verbatim raw body", carrier.UpstreamBody)
	}
	if carrier.ChatErr != nil {
		t.Fatalf("ChatErr = %v, want nil", carrier.ChatErr)
	}
}

// TestEnsureBodyIngestsAnthropicMessages: the translated wire stores
// the canonical encoding as the upstream body.
func TestEnsureBodyIngestsAnthropicMessages(t *testing.T) {
	t.Parallel()

	raw := `{"model":"claude-3","max_tokens":64,"messages":[{"role":"user","content":"hi"}]}`
	rec, carrier, ok := runEnsureBody(t, protocol.FormatAnthropicMessages, strings.NewReader(raw), nil)

	if !ok {
		t.Fatalf("EnsureBody = false, body: %s", rec.Body.String())
	}
	if carrier.Chat.Model != "claude-3" || carrier.Chat.MaxTokens == nil || *carrier.Chat.MaxTokens != 64 {
		t.Fatalf("canonical chat = %+v, want model claude-3 with max_tokens 64", carrier.Chat)
	}
	if string(carrier.UpstreamBody) == raw {
		t.Fatal("UpstreamBody must be the canonical re-encoding, not the raw messages body")
	}
	chat, err := protocol.ParseChatRequest(carrier.UpstreamBody)
	if err != nil || chat.Model != "claude-3" || len(chat.Messages) != 1 {
		t.Fatalf("UpstreamBody %q is not canonical chat: %v", carrier.UpstreamBody, err)
	}
}

// TestEnsureBodyRejectsMalformedJSON: a body that fails the wire's
// ingest renders 400 in the carrier's format.
func TestEnsureBodyRejectsMalformedJSON(t *testing.T) {
	t.Parallel()

	rec, _, ok := runEnsureBody(t, protocol.FormatOpenAIChat, strings.NewReader(`{"model":`), nil)

	if ok {
		t.Fatal("EnsureBody = true, want false for a malformed body")
	}
	assertEnvelope(t, rec, http.StatusBadRequest, "invalid_request")
}

// TestEnsureBodyRejectsOversizedBody: a body past the cap renders 413.
func TestEnsureBodyRejectsOversizedBody(t *testing.T) {
	t.Parallel()

	oversized := strings.Repeat("a", protocol.MaxBodyBytes+1)
	rec, _, ok := runEnsureBody(t, protocol.FormatOpenAIChat, strings.NewReader(oversized), nil)

	if ok {
		t.Fatal("EnsureBody = true, want false past the body cap")
	}
	assertEnvelope(t, rec, http.StatusRequestEntityTooLarge, "request_too_large")
}

// TestEnsureBodyRejectsUnreadableBody: a read failure that is not the
// size cap renders 400, not 413.
func TestEnsureBodyRejectsUnreadableBody(t *testing.T) {
	t.Parallel()

	rec, _, ok := runEnsureBody(t, protocol.FormatOpenAIChat, errReader{err: errors.New("disk gone")}, nil)

	if ok {
		t.Fatal("EnsureBody = true, want false for an unreadable body")
	}
	assertEnvelope(t, rec, http.StatusBadRequest, "invalid_request")
	if strings.Contains(rec.Body.String(), "too large") {
		t.Fatalf("read failure misreported as oversize: %s", rec.Body.String())
	}
}

// TestFormatStagePinsFormat: the stage writes the route's format onto
// the carrier before the body is read.
func TestFormatStagePinsFormat(t *testing.T) {
	t.Parallel()

	var seen protocol.Format
	handler := FormatStage(protocol.FormatAnthropicMessages)(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			seen = CarrierFrom(r.Context()).Format
		}))

	req := httptest.NewRequest(http.MethodPost, "/v1/messages", nil).
		WithContext(WithCarrier(context.Background(), &Carrier{}))
	handler.ServeHTTP(httptest.NewRecorder(), req)

	if seen != protocol.FormatAnthropicMessages {
		t.Fatalf("carrier format = %q, want anthropic-messages", seen)
	}
}

// TestFormatStageToleratesMissingCarrier: the stage never panics when
// composed without the carrier stage.
func TestFormatStageToleratesMissingCarrier(t *testing.T) {
	t.Parallel()

	served := false
	handler := FormatStage(protocol.FormatOpenAIChat)(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) { served = true }))

	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", nil))
	if !served {
		t.Fatal("downstream handler was not served")
	}
}
