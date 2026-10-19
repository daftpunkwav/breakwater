/**
 * @file anthropic_test
 * @description Messages wire tests: ingest rules (max_tokens required,
 * stop_sequences refused), success re-rendering, the translated event
 * sequence and the abort frame.
 */
package protocol

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func headerOf(contentType string) http.Header {
	header := http.Header{}
	header.Set("Content-Type", contentType)
	return header
}

func TestIngestAnthropic(t *testing.T) {
	t.Parallel()
	result, err := Ingest(FormatAnthropicMessages, []byte(`{
		"model": "m",
		"max_tokens": 128,
		"system": "be terse",
		"messages": [
			{"role": "user", "content": [{"type": "text", "text": "hi "}, {"type": "text", "text": "there"}]}
		]
	}`))
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if len(result.Chat.Messages) != 2 {
		t.Fatalf("messages = %+v, want system + 1", result.Chat.Messages)
	}
	if result.Chat.Messages[0].Content != "be terse" {
		t.Fatalf("system lost: %+v", result.Chat.Messages[0])
	}
	if result.Chat.Messages[1].Content != "hi there" {
		t.Fatalf("block text not joined: %+v", result.Chat.Messages[1])
	}
	if result.Chat.MaxTokens == nil || *result.Chat.MaxTokens != 128 {
		t.Fatalf("max_tokens not mapped")
	}
}

func TestIngestAnthropicStreamRequestsUsage(t *testing.T) {
	t.Parallel()
	requireIngestedStreamRequestsUsage(t, FormatAnthropicMessages,
		`{"model":"m","stream":true,"max_tokens":16,"messages":[{"role":"user","content":"hi"}]}`)
}

func TestIngestAnthropicRequiresMaxTokens(t *testing.T) {
	t.Parallel()
	if _, err := Ingest(FormatAnthropicMessages, []byte(`{"model":"m","messages":[{"role":"user","content":"hi"}]}`)); err == nil {
		t.Fatal("missing max_tokens must be rejected: the gateway refuses to guess a spend bound")
	}
}

func TestIngestAnthropicRefusesStopSequences(t *testing.T) {
	t.Parallel()
	_, err := Ingest(FormatAnthropicMessages, []byte(
		`{"model":"m","max_tokens":10,"stop_sequences":["END"],"messages":[{"role":"user","content":"hi"}]}`))
	if err == nil {
		t.Fatal("silently dropping stop_sequences would change semantics")
	}
}

func TestAnthropicWireRenderSuccess(t *testing.T) {
	t.Parallel()
	upstream := []byte(`{"id":"chatcmpl-1","model":"m","choices":[{"message":{"role":"assistant","content":"the answer"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}`)

	rec := httptest.NewRecorder()
	WireFor(FormatAnthropicMessages).RenderSuccess(rec, 200, headerOf("application/json"), upstream)

	var body struct {
		ID         string `json:"id"`
		Type       string `json:"type"`
		Role       string `json:"role"`
		StopReason string `json:"stop_reason"`
		Content    []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
		Usage struct {
			InputTokens  int64 `json:"input_tokens"`
			OutputTokens int64 `json:"output_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Type != "message" || body.Role != "assistant" || body.StopReason != "end_turn" ||
		len(body.Content) != 1 || body.Content[0].Text != "the answer" {
		t.Fatalf("body = %s", rec.Body.String())
	}
	if body.Usage.OutputTokens != 2 || body.Usage.InputTokens != 3 {
		t.Fatalf("usage = %+v", body.Usage)
	}
}

func TestAnthropicWireRenderError(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	WireFor(FormatAnthropicMessages).RenderError(rec, 429, "rate_limit_exceeded", "slow down")
	if rec.Code != 429 {
		t.Fatalf("status = %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"type":"error"`) ||
		!strings.Contains(rec.Body.String(), `"type":"rate_limit_error"`) {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

// TestAnthropicConcurrencyRejectionIsARateLimit pins that both of the
// gateway's 429 ceilings answer with the one type the Messages
// vocabulary has for that status. A concurrency rejection carries
// Retry-After guidance exactly like a rate limit, so rendering it
// api_error would tell a client to give up on a wait it was handed.
func TestAnthropicConcurrencyRejectionIsARateLimit(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	WireFor(FormatAnthropicMessages).RenderError(rec, http.StatusTooManyRequests,
		string(CodeConcurrencyLimited), "tenant concurrency limit exceeded")
	if got := rec.Body.String(); !strings.Contains(got, `"type":"rate_limit_error"`) {
		t.Fatalf("body = %s, want rate_limit_error", got)
	}
}

// TestAnthropicUpstreamErrorForwardsRetryAfter pins that a relayed
// upstream 429 keeps both halves of its own wait guidance on this wire:
// the header and the rate-limit type. The other two wires forward the
// header through the shared passthrough set.
func TestAnthropicUpstreamErrorForwardsRetryAfter(t *testing.T) {
	t.Parallel()
	header := headerOf("application/json")
	header.Set("Retry-After", "7")
	header.Set("X-Internal", "secret")
	rec := httptest.NewRecorder()

	WireFor(FormatAnthropicMessages).RenderUpstreamError(rec, http.StatusTooManyRequests,
		header, []byte(`{"error":{"message":"slow down","code":"429"}}`))

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d", rec.Code)
	}
	if got := rec.Header().Get("Retry-After"); got != "7" {
		t.Fatalf("retry-after = %q, want the upstream wait forwarded", got)
	}
	if got := rec.Body.String(); !strings.Contains(got, `"type":"rate_limit_error"`) {
		t.Fatalf("body = %s, want rate_limit_error", got)
	}
	if got := rec.Header().Get("X-Internal"); got != "" {
		t.Fatalf("internal header leaked: %q", got)
	}
}

// TestAnthropicUpstreamErrorRejectsIllegalRetryAfter pins that the
// forwarded value clears the same legality check the passthrough set
// applies: the header is upstream-controlled.
func TestAnthropicUpstreamErrorRejectsIllegalRetryAfter(t *testing.T) {
	t.Parallel()
	header := headerOf("application/json")
	header.Set("Retry-After", "3\r\nSet-Cookie: injected=1")
	rec := httptest.NewRecorder()

	WireFor(FormatAnthropicMessages).RenderUpstreamError(rec, http.StatusServiceUnavailable,
		header, []byte(`{"error":{"message":"overloaded"}}`))

	if got := rec.Header().Get("Retry-After"); got != "" {
		t.Fatalf("illegal retry-after relayed: %q", got)
	}
	if got := rec.Body.String(); !strings.Contains(got, `"type":"api_error"`) {
		t.Fatalf("body = %s, want api_error for a non-429", got)
	}
}

// startAnthropicStream opens the Anthropic stream transcoder over a
// fresh recorder and emits the message_start preamble; it returns both
// so the tests drive deltas and read the emitted frames.
func startAnthropicStream(t *testing.T) (*httptest.ResponseRecorder, StreamTranscoder) {
	t.Helper()
	rec := httptest.NewRecorder()
	transcoder := WireFor(FormatAnthropicMessages).Stream()
	if err := transcoder.Start(rec, "m"); err != nil {
		t.Fatalf("start: %v", err)
	}
	return rec, transcoder
}

func TestAnthropicStreamSequence(t *testing.T) {
	t.Parallel()
	rec, transcoder := startAnthropicStream(t)

	if err := transcoder.Delta(rec, []byte(`{"choices":[{"delta":{"role":"assistant"}}]}`)); err != nil {
		t.Fatalf("role chunk: %v", err)
	}
	if err := transcoder.Delta(rec, []byte(`{"choices":[{"delta":{"content":"he"}}]}`)); err != nil {
		t.Fatalf("delta: %v", err)
	}
	if err := transcoder.Delta(rec, []byte(`{"choices":[{"delta":{"content":"llo"},"finish_reason":"stop"}]}`)); err != nil {
		t.Fatalf("delta: %v", err)
	}
	if err := transcoder.Delta(rec, []byte(`{"choices":[],"usage":{"prompt_tokens":1,"completion_tokens":2,"total_tokens":3}}`)); err != nil {
		t.Fatalf("usage chunk: %v", err)
	}
	if err := transcoder.Finish(rec, Usage{PromptTokens: 1, CompletionTokens: 2, TotalTokens: 3}, true); err != nil {
		t.Fatalf("finish: %v", err)
	}

	body := rec.Body.String()
	for _, want := range []string{
		"event: message_start",
		"event: content_block_start",
		"event: content_block_delta",
		`"text":"he"`,
		`"text":"llo"`,
		"event: content_block_stop",
		"event: message_delta",
		`"stop_reason":"end_turn"`,
		`"output_tokens":2`,
		"event: message_stop",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("stream missing %q\ngot:\n%s", want, body)
		}
	}
	// The text block must open exactly once, before the first delta.
	if first := strings.Index(body, "content_block_start"); first == -1 ||
		first > strings.Index(body, "content_block_delta") {
		t.Errorf("block start must precede deltas:\n%s", body)
	}
}

// TestAnthropicStreamFinishReasonOnCombinedFrame pins that a
// finish_reason riding the last content frame — the shape vLLM-style
// compatible backends emit — still maps onto the stop_reason vocabulary
// instead of degrading to end_turn.
func TestAnthropicStreamFinishReasonOnCombinedFrame(t *testing.T) {
	t.Parallel()
	rec, transcoder := startAnthropicStream(t)

	if err := transcoder.Delta(rec, []byte(`{"choices":[{"delta":{"content":"cut short"},"finish_reason":"length"}]}`)); err != nil {
		t.Fatalf("delta: %v", err)
	}
	if err := transcoder.Finish(rec, Usage{}, false); err != nil {
		t.Fatalf("finish: %v", err)
	}
	if !strings.Contains(rec.Body.String(), `"stop_reason":"max_tokens"`) {
		t.Fatalf("combined-frame finish_reason lost:\n%s", rec.Body.String())
	}
}

func TestAnthropicStreamAbort(t *testing.T) {
	t.Parallel()
	rec, transcoder := startAnthropicStream(t)
	if err := transcoder.Abort(rec, CodeUpstreamTimeout, "deadline"); err != nil {
		t.Fatalf("abort: %v", err)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "event: error") || !strings.Contains(body, `"type":"timeout_error"`) {
		t.Fatalf("abort stream = %s", body)
	}
}
