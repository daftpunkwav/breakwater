/**
 * @file wire_dispatch_test
 * @description The wire dispatch table: unknown formats fall back to the
 * canonical wire, the canonical wire's identity rendering, and Ingest's
 * format routing.
 */
package protocol

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWireForUnknownFormatFallsBackToCanonical(t *testing.T) {
	t.Parallel()
	for _, format := range []Format{Format(""), Format("graphql"), Format("OPENAI-CHAT")} {
		if wire := WireFor(format); wire.Format() != FormatOpenAIChat {
			t.Fatalf("WireFor(%q) = %s, want the canonical fallback", format, wire.Format())
		}
	}
}

func TestWireFormats(t *testing.T) {
	t.Parallel()
	cases := map[Format]Format{
		FormatOpenAIChat:        FormatOpenAIChat,
		FormatOpenAIResponses:   FormatOpenAIResponses,
		FormatAnthropicMessages: FormatAnthropicMessages,
	}
	for format, want := range cases {
		if got := WireFor(format).Format(); got != want {
			t.Errorf("WireFor(%s).Format() = %s, want %s", format, got, want)
		}
	}
}

func TestChatWireStreamsRawBytes(t *testing.T) {
	t.Parallel()
	// The canonical wire needs no translation: nil tells the pump to pass
	// SSE bytes through untouched.
	if wire := WireFor(FormatOpenAIChat); wire.Stream() != nil {
		t.Fatalf("canonical wire stream = %+v, want nil", wire.Stream())
	}
}

func TestChatWireRenderUpstreamErrorPassthrough(t *testing.T) {
	t.Parallel()
	upstream := []byte(`{"error":{"message":"overloaded","code":"5xx"}}`)
	header := headerOf("application/json")
	header.Set("Retry-After", "9")
	header.Set("X-Internal", "secret")
	rec := httptest.NewRecorder()

	chatWire{}.RenderUpstreamError(rec, http.StatusBadGateway, header, upstream)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d", rec.Code)
	}
	if got := rec.Header().Get("Retry-After"); got != "9" {
		t.Fatalf("retry-after = %q, want passthrough", got)
	}
	if got := rec.Header().Get("X-Internal"); got != "" {
		t.Fatalf("internal header leaked: %q", got)
	}
	if got := rec.Body.String(); got != string(upstream) {
		t.Fatalf("body = %q, want verbatim %q", got, upstream)
	}
}

func TestIngestCanonicalFormatIsBytePassthrough(t *testing.T) {
	t.Parallel()
	body := []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}],"temperature":0.5}`)
	result, err := Ingest(FormatOpenAIChat, body)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	// The canonical wire forwards the original bytes: unknown fields are
	// preserved and never re-encoded.
	if string(result.UpstreamBody) != string(body) {
		t.Fatalf("upstream body = %s, want verbatim", result.UpstreamBody)
	}
	if result.Chat.Model != "m" || len(result.Chat.Messages) != 1 {
		t.Fatalf("canonical = %+v", result.Chat)
	}
}

func TestIngestUnknownFormatUsesCanonicalParse(t *testing.T) {
	t.Parallel()
	result, err := Ingest(Format("sse-v9"), []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if result.Chat.Model != "m" {
		t.Fatalf("canonical = %+v", result.Chat)
	}
}

func TestIngestCanonicalFormatRejectsMalformedBody(t *testing.T) {
	t.Parallel()
	if _, err := Ingest(FormatOpenAIChat, []byte("{not json")); err == nil {
		t.Fatal("malformed body must be rejected")
	}
}

func TestParseChatResponseRejectsUnusableBodies(t *testing.T) {
	t.Parallel()
	for name, body := range map[string][]byte{
		"malformed":     []byte("{not json"),
		"no choices":    []byte(`{"id":"x","model":"m"}`),
		"empty choices": []byte(`{"choices":[]}`),
	} {
		if _, ok := parseChatResponse(body); ok {
			t.Errorf("%s: body accepted as a chat completion", name)
		}
	}
}
