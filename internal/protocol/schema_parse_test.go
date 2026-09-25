/**
 * @file schema_parse_test
 * @description Request and usage parsing of the canonical schema, plus
 * the ingest rejection rules both translated wires apply on top.
 */
package protocol

import (
	"strings"
	"testing"
)

func TestParseChatRequestRejectsMalformedBody(t *testing.T) {
	t.Parallel()
	if _, err := ParseChatRequest([]byte("{not json")); err == nil {
		t.Fatal("malformed body must be rejected")
	}
}

func TestParseUsage(t *testing.T) {
	t.Parallel()
	usage, ok := ParseUsage([]byte(`{"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}`))
	if !ok || usage.PromptTokens != 3 || usage.CompletionTokens != 2 || usage.TotalTokens != 5 {
		t.Fatalf("usage = %+v ok = %v", usage, ok)
	}
	// Providers that omit usage exist; settlement needs the explicit
	// signal, never a silent zero.
	if _, ok := ParseUsage([]byte(`{"id":"x"}`)); ok {
		t.Error("missing usage must report not-ok")
	}
	if _, ok := ParseUsage([]byte("{not json")); ok {
		t.Error("malformed body must report not-ok")
	}
}

func TestIngestResponsesRejections(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"malformed":     `{`,
		"model missing": `{"input":"hi"}`,
		"input type":    `{"model":"m","input":5}`,
		"content type":  `{"model":"m","input":[{"role":"user","content":123}]}`,
	}
	for name, body := range cases {
		if _, err := Ingest(FormatOpenAIResponses, []byte(body)); err == nil {
			t.Errorf("%s: responses ingest accepted %s", name, body)
		}
	}
}

func TestIngestAnthropicRejections(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"malformed":     `{`,
		"model missing": `{"max_tokens":8,"messages":[]}`,
		"content type":  `{"model":"m","max_tokens":8,"messages":[{"role":"user","content":123}]}`,
		"system type":   `{"model":"m","max_tokens":8,"system":5,"messages":[]}`,
	}
	for name, body := range cases {
		if _, err := Ingest(FormatAnthropicMessages, []byte(body)); err == nil {
			t.Errorf("%s: messages ingest accepted %s", name, body)
		}
	}
}

func TestIngestAnthropicSkipsEmptySystem(t *testing.T) {
	t.Parallel()
	result, err := Ingest(FormatAnthropicMessages, []byte(
		`{"model":"m","max_tokens":8,"system":"","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	if len(result.Chat.Messages) != 1 || result.Chat.Messages[0].Role == "system" {
		t.Fatalf("messages = %+v, want no empty system message", result.Chat.Messages)
	}
}

func TestIngestResponsesContentMustBeTextParts(t *testing.T) {
	t.Parallel()
	_, err := Ingest(FormatOpenAIResponses, []byte(`{"model":"m","input":[{"role":"user","content":[{"type":"input_image","image_url":"x"}]}]}`))
	if err == nil || !strings.Contains(err.Error(), "input_image") {
		t.Fatalf("err = %v, want the unsupported part named", err)
	}
}

func TestIngestResponsesStreamRequestsUsage(t *testing.T) {
	t.Parallel()
	result, err := Ingest(FormatOpenAIResponses, []byte(
		`{"model":"m","stream":true,"input":"hi","max_output_tokens":16}`))
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	chat, err := ParseChatRequest(result.UpstreamBody)
	if err != nil {
		t.Fatalf("canonical body: %v", err)
	}
	if chat.StreamOptions == nil || !chat.StreamOptions.IncludeUsage {
		t.Fatalf("canonical body = %s, want stream_options.include_usage", result.UpstreamBody)
	}
}

func TestIngestAnthropicRejectsNonTextBlocks(t *testing.T) {
	t.Parallel()
	_, err := Ingest(FormatAnthropicMessages, []byte(
		`{"model":"m","max_tokens":8,"messages":[{"role":"user","content":[{"type":"tool_use","id":"t1"}]}]}`))
	if err == nil || !strings.Contains(err.Error(), "tool_use") {
		t.Fatalf("err = %v, want the unsupported block named", err)
	}
}
