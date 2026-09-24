/**
 * @file anthropic
 * @description The Anthropic Messages wire: POST /v1/messages ingress
 * translated through the canonical chat form.
 *
 * Responsibilities:
 * - Ingest Messages requests (messages with string or text-block
 *   content, system, required max_tokens) into the canonical request
 * - Re-render upstream chat replies as Messages objects
 * - Translate the upstream chat SSE sequence into the Messages event
 *   stream: message_start, content_block_start, content_block_delta
 *   per text piece, content_block_stop, message_delta with stop reason
 *   and usage, message_stop; the error event on mid-stream faults
 *
 * Scope honesty: text content only — tool_use, image and other block
 * types are rejected at ingest with a clear error. Auth is x-api-key
 * (handled by the auth stage); anthropic-version is accepted and not
 * enforced.
 */
package protocol

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// anthropicRequest is the subset of the Messages schema the gateway
// consumes.
type anthropicRequest struct {
	Model         string          `json:"model"`
	Stream        bool            `json:"stream"`
	System        json.RawMessage `json:"system"`
	Messages      []anthropicMsg  `json:"messages"`
	MaxTokens     *int64          `json:"max_tokens"`
	Temperature   *float64        `json:"temperature,omitempty"`
	TopP          *float64        `json:"top_p,omitempty"`
	StopSequences []string        `json:"stop_sequences,omitempty"`
}

// anthropicMsg is one conversation entry with flexible content.
type anthropicMsg struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}

// ingestAnthropic parses a Messages request into the canonical form.
func ingestAnthropic(body []byte) (IngestResult, error) {
	var req anthropicRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return IngestResult{}, fmt.Errorf("malformed messages request: %w", err)
	}
	if req.Model == "" {
		return IngestResult{}, fmt.Errorf("model is required")
	}
	if req.MaxTokens == nil || *req.MaxTokens <= 0 {
		// Anthropic requires max_tokens; the gateway refuses to guess a
		// spend bound the client never declared.
		return IngestResult{}, fmt.Errorf("max_tokens is required and must be positive")
	}
	if len(req.StopSequences) > 0 {
		// Silently dropping stop sequences would change what the client
		// gets; refuse instead.
		return IngestResult{}, fmt.Errorf("stop_sequences is not supported by this gateway")
	}

	chat := ChatRequest{
		Model:       req.Model,
		Stream:      req.Stream,
		MaxTokens:   req.MaxTokens,
		Temperature: req.Temperature,
		TopP:        req.TopP,
	}

	if len(req.System) > 0 {
		system, err := anthropicText(req.System)
		if err != nil {
			return IngestResult{}, err
		}
		if system != "" {
			chat.Messages = append(chat.Messages, ChatMessage{Role: "system", Content: system})
		}
	}
	for _, msg := range req.Messages {
		text, err := anthropicText(msg.Content)
		if err != nil {
			return IngestResult{}, err
		}
		chat.Messages = append(chat.Messages, ChatMessage{Role: msg.Role, Content: text})
	}

	upstream, err := json.Marshal(chat)
	if err != nil {
		return IngestResult{}, fmt.Errorf("encode canonical request: %w", err)
	}
	return IngestResult{Chat: chat, UpstreamBody: upstream}, nil
}

// anthropicText extracts text from a content value: a plain string or
// an array of blocks with text blocks only.
func anthropicText(raw json.RawMessage) (string, error) {
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return text, nil
	}
	var blocks []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &blocks); err != nil {
		return "", fmt.Errorf("content must be a string or a block array")
	}
	var joined strings.Builder
	for _, block := range blocks {
		if block.Type != "text" {
			return "", fmt.Errorf("unsupported content block type %q: text blocks only", block.Type)
		}
		joined.WriteString(block.Text)
	}
	return joined.String(), nil
}

// anthropicWire renders the Messages surface.
type anthropicWire struct{}

// Format implements Wire.
func (anthropicWire) Format() Format { return FormatAnthropicMessages }

// RenderSuccess implements Wire: the upstream chat reply re-rendered as
// a Messages object.
func (anthropicWire) RenderSuccess(w http.ResponseWriter, status int, header http.Header, upstreamBody []byte) {
	resp, ok := parseChatResponse(upstreamBody)
	if !ok {
		renderExchangeBody(w, status, header, upstreamBody, passthroughHeaderNames)
		return
	}
	usage := Usage{}
	usageKnown := false
	if resp.Usage != nil {
		usage, usageKnown = *resp.Usage, true
	}
	writeJSONResponse(w, status, anthropicObject(resp.ID, resp.Model, resp.Choices[0].Message.Content,
		anthropicStopReason(resp.Choices[0].FinishReason), usage, usageKnown))
}

// RenderUpstreamError implements Wire: the upstream OpenAI error body
// re-rendered in the Messages error envelope, same status.
func (anthropicWire) RenderUpstreamError(w http.ResponseWriter, status int, header http.Header, upstreamBody []byte) {
	message, _ := parseUpstreamError(upstreamBody)
	writeJSONResponse(w, status, anthropicErrorBody("api_error", message))
}

// RenderError implements Wire: gateway failures use the Messages error
// envelope with the frozen in-stream code preserved in the type.
func (anthropicWire) RenderError(w http.ResponseWriter, status int, code, message string) {
	writeJSONResponse(w, status, anthropicErrorBody(anthropicErrorType(Code(code)), message))
}

// Stream implements Wire.
func (anthropicWire) Stream() StreamTranscoder { return &anthropicStream{id: newGatewayID("msg_gw_")} }

// anthropicObject builds the completed Messages object.
func anthropicObject(id, model, text, stopReason string, usage Usage, usageKnown bool) map[string]any {
	obj := map[string]any{
		"id":    id,
		"type":  "message",
		"role":  "assistant",
		"model": model,
		"content": []map[string]any{{
			"type": "text",
			"text": text,
		}},
		"stop_reason":   stopReason,
		"stop_sequence": nil,
	}
	if usageKnown {
		obj["usage"] = map[string]any{
			"input_tokens":  usage.PromptTokens,
			"output_tokens": usage.CompletionTokens,
		}
	}
	return obj
}

// anthropicStopReason maps an OpenAI finish reason to the Messages
// stop_reason vocabulary.
func anthropicStopReason(finish string) string {
	switch finish {
	case "length":
		return "max_tokens"
	case "tool_calls":
		return "tool_use"
	default:
		return "end_turn"
	}
}

// anthropicErrorType maps a frozen in-stream code to the Messages
// error-type vocabulary.
func anthropicErrorType(code Code) string {
	switch code {
	case CodeUpstreamTimeout:
		return "timeout_error"
	case "rate_limit_exceeded":
		return "rate_limit_error"
	case "insufficient_quota":
		return "billing_error"
	default:
		return "api_error"
	}
}

// anthropicErrorBody builds the Messages error envelope.
func anthropicErrorBody(errType, message string) map[string]any {
	return map[string]any{
		"type":  "error",
		"error": map[string]any{"type": errType, "message": message},
	}
}

// anthropicStream is the stateful Messages event translator.
type anthropicStream struct {
	id    string
	model string
	block bool // content block opened
	text  []byte
	stop  string
}

// Start implements StreamTranscoder: message_start with a zeroed
// usage — input tokens are unknown until the upstream's final chunk.
func (s *anthropicStream) Start(w ioWriter, model string) error {
	s.model = model
	return WriteEvent(w, "message_start", map[string]any{
		"type": "message_start",
		"message": map[string]any{
			"id": s.id, "type": "message", "role": "assistant", "model": model,
			"content": []any{},
			"usage":   map[string]any{"input_tokens": 0, "output_tokens": 0},
		},
	})
}

// Delta implements StreamTranscoder: the first content opens the text
// block; every piece is a content_block_delta. A finish_reason is
// captured wherever it rides — some compatible backends send it on the
// same frame as the last piece of content instead of a separate one.
func (s *anthropicStream) Delta(w ioWriter, payload []byte) error {
	var chunk struct {
		Choices []struct {
			Delta struct {
				Content string `json:"content"`
			} `json:"delta"`
			FinishReason *string `json:"finish_reason"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(payload, &chunk); err != nil || len(chunk.Choices) == 0 {
		return nil
	}

	if chunk.Choices[0].FinishReason != nil {
		s.stop = anthropicStopReason(*chunk.Choices[0].FinishReason)
	}

	if text := chunk.Choices[0].Delta.Content; text != "" {
		if !s.block {
			s.block = true
			if err := WriteEvent(w, "content_block_start", map[string]any{
				"type":          "content_block_start",
				"index":         0,
				"content_block": map[string]any{"type": "text", "text": ""},
			}); err != nil {
				return err
			}
		}
		s.text = append(s.text, text...)
		return WriteEvent(w, "content_block_delta", map[string]any{
			"type":  "content_block_delta",
			"index": 0,
			"delta": map[string]any{"type": "text_delta", "text": text},
		})
	}
	return nil
}

// Finish implements StreamTranscoder: block stop, message_delta with
// stop reason and usage, message_stop.
func (s *anthropicStream) Finish(w ioWriter, usage Usage, usageKnown bool) error {
	if s.block {
		if err := WriteEvent(w, "content_block_stop", map[string]any{
			"type": "content_block_stop", "index": 0,
		}); err != nil {
			return err
		}
	}
	if s.stop == "" {
		s.stop = "end_turn"
	}
	outputTokens := int64(0)
	if usageKnown {
		outputTokens = usage.CompletionTokens
	}
	if err := WriteEvent(w, "message_delta", map[string]any{
		"type":  "message_delta",
		"delta": map[string]any{"stop_reason": s.stop, "stop_sequence": nil},
		"usage": map[string]any{"output_tokens": outputTokens},
	}); err != nil {
		return err
	}
	return WriteEvent(w, "message_stop", map[string]any{"type": "message_stop"})
}

// Abort implements StreamTranscoder: the Messages error event, then
// the stream ends (the Messages contract has no [DONE] sentinel).
func (s *anthropicStream) Abort(w ioWriter, code Code, message string) error {
	return WriteEvent(w, "error", anthropicErrorBody(anthropicErrorType(Code(code)), message))
}
