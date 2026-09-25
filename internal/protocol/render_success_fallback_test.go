/**
 * @file render_success_fallback_test
 * @description The success-render fallbacks: bodies that are not
 * parseable chat completions pass through instead of being guessed at,
 * and a missing usage is dropped honestly.
 */
package protocol

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
)

// chatCompletionBody is a minimal upstream reply the translated wires
// re-render.
const chatCompletionBody = `{"id":"chatcmpl-1","model":"m","choices":[{"message":{"role":"assistant","content":"the answer"},"finish_reason":"stop"}]}`

func TestTranslatedWiresPassThroughUnparseableSuccessBodies(t *testing.T) {
	t.Parallel()
	upstream := []byte(`{"unexpected":"shape"}`)
	for name, wire := range map[string]Wire{
		"responses": responsesWire{},
		"anthropic": anthropicWire{},
	} {
		rec := httptest.NewRecorder()
		wire.RenderSuccess(rec, 200, headerOf("text/event-stream"), upstream)
		if rec.Code != 200 {
			t.Errorf("%s: status = %d", name, rec.Code)
		}
		if got := rec.Body.String(); got != string(upstream) {
			t.Errorf("%s: body = %q, want verbatim passthrough", name, got)
		}
	}
}

func TestAnthropicRenderSuccessDropsMissingUsage(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	anthropicWire{}.RenderSuccess(rec, 200, headerOf("application/json"), []byte(chatCompletionBody))

	var body struct {
		Usage *struct {
			InputTokens int64 `json:"input_tokens"`
		} `json:"usage"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if body.Usage != nil {
		t.Fatalf("usage = %+v, want no usage key when the upstream reported none", body.Usage)
	}
	if !strings.Contains(rec.Body.String(), `"text":"the answer"`) {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestAnthropicStopReasonVocabulary(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"length":     "max_tokens",
		"tool_calls": "tool_use",
		"stop":       "end_turn",
		"":           "end_turn",
		"exotic":     "end_turn",
	}
	for finish, want := range cases {
		if got := anthropicStopReason(finish); got != want {
			t.Errorf("stop reason for %q = %q, want %q", finish, got, want)
		}
	}
}
