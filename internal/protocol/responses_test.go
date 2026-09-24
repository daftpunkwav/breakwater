/**
 * @file responses_test
 * @description Responses wire tests: ingest into the canonical form,
 * success re-rendering, and the translated event sequence.
 */
package protocol

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestIngestResponsesStringInput(t *testing.T) {
	t.Parallel()
	result, err := Ingest(FormatOpenAIResponses,
		[]byte(`{"model":"m","input":"hello there","max_output_tokens":64}`))
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if result.Chat.Model != "m" || len(result.Chat.Messages) != 1 ||
		result.Chat.Messages[0].Role != "user" || result.Chat.Messages[0].Content != "hello there" {
		t.Fatalf("canonical = %+v", result.Chat)
	}
	if result.Chat.MaxTokens == nil || *result.Chat.MaxTokens != 64 {
		t.Fatalf("max_output_tokens not mapped: %+v", result.Chat.MaxTokens)
	}
	// The forwardable body is the canonical encoding: the chat fields
	// survive a re-parse.
	chat, err := ParseChatRequest(result.UpstreamBody)
	if err != nil || chat.Model != "m" || len(chat.Messages) != 1 {
		t.Fatalf("upstream body = %s err = %v", result.UpstreamBody, err)
	}
}

func TestIngestResponsesArrayInputAndInstructions(t *testing.T) {
	t.Parallel()
	result, err := Ingest(FormatOpenAIResponses, []byte(`{
		"model": "m",
		"instructions": "be terse",
		"input": [
			{"role": "user", "content": [{"type": "input_text", "text": "hi"}]},
			{"role": "assistant", "content": "hello"}
		]
	}`))
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if len(result.Chat.Messages) != 3 {
		t.Fatalf("messages = %+v, want system + 2", result.Chat.Messages)
	}
	if result.Chat.Messages[0].Role != "system" || result.Chat.Messages[0].Content != "be terse" {
		t.Fatalf("instructions not leading: %+v", result.Chat.Messages[0])
	}
	if result.Chat.Messages[1].Content != "hi" {
		t.Fatalf("part text lost: %+v", result.Chat.Messages[1])
	}
}

func TestIngestResponsesRejectsUnsupportedParts(t *testing.T) {
	t.Parallel()
	if _, err := Ingest(FormatOpenAIResponses, []byte(`{
		"model": "m",
		"input": [{"role": "user", "content": [{"type": "input_image", "image_url": "x"}]}]
	}`)); err == nil {
		t.Fatal("image part must be rejected loudly")
	}
}

func TestResponsesWireRenderSuccess(t *testing.T) {
	t.Parallel()
	upstream := []byte(`{"id":"chatcmpl-1","model":"m","choices":[{"message":{"role":"assistant","content":"the answer"},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}`)

	rec := httptest.NewRecorder()
	WireFor(FormatOpenAIResponses).RenderSuccess(rec, 200, headerOf("application/json"), upstream)

	if rec.Code != 200 {
		t.Fatalf("status = %d", rec.Code)
	}
	var body struct {
		ID     string `json:"id"`
		Object string `json:"object"`
		Status string `json:"status"`
		Output []struct {
			Type    string `json:"type"`
			Content []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"content"`
		} `json:"output"`
		Usage *struct {
			InputTokens  int64 `json:"input_tokens"`
			OutputTokens int64 `json:"output_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Object != "response" || body.Status != "completed" || len(body.Output) != 1 ||
		body.Output[0].Content[0].Text != "the answer" {
		t.Fatalf("body = %s", rec.Body.String())
	}
	if body.Usage == nil || body.Usage.OutputTokens != 2 {
		t.Fatalf("usage = %+v", body.Usage)
	}
}

func TestResponsesStreamSequence(t *testing.T) {
	t.Parallel()
	transcoder := WireFor(FormatOpenAIResponses).Stream()
	rec := httptest.NewRecorder()

	if err := transcoder.Start(rec, "m"); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := transcoder.Delta(rec, []byte(`{"choices":[{"delta":{"content":"he"}}]}`)); err != nil {
		t.Fatalf("delta: %v", err)
	}
	if err := transcoder.Delta(rec, []byte(`{"choices":[{"delta":{"content":"llo"}}]}`)); err != nil {
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
		"event: response.created",
		`"status":"in_progress"`,
		"event: response.output_text.delta",
		`"delta":"he"`,
		`"delta":"llo"`,
		"event: response.completed",
		`"text":"hello"`,
		`"output_tokens":2`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("stream missing %q\ngot:\n%s", want, body)
		}
	}
}

func TestResponsesStreamAbort(t *testing.T) {
	t.Parallel()
	transcoder := WireFor(FormatOpenAIResponses).Stream()
	rec := httptest.NewRecorder()
	if err := transcoder.Start(rec, "m"); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := transcoder.Abort(rec, CodeUpstreamReset, "connection reset"); err != nil {
		t.Fatalf("abort: %v", err)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "event: response.failed") ||
		!strings.Contains(body, `"code":"upstream_reset"`) {
		t.Fatalf("abort stream = %s", body)
	}
}
