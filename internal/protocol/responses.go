/**
 * @file responses
 * @description The OpenAI Responses wire: POST /v1/responses ingress
 * translated through the canonical chat form.
 *
 * Responsibilities:
 * - Ingest Responses requests (input string or message array,
 *   instructions, max_output_tokens) into the canonical request
 * - Re-render upstream chat replies as Responses objects
 * - Translate the upstream chat SSE sequence into the Responses event
 *   stream: response.created, response.output_text.delta per text
 *   piece, response.completed with usage; response.failed on mid-stream
 *   faults
 *
 * Scope honesty: text content only — input parts and output types the
 * mock/text world cannot produce are rejected at ingest with a clear
 * error instead of being silently mangled.
 */
package protocol

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
)

// responsesRequest is the subset of the Responses schema the gateway
// consumes.
type responsesRequest struct {
	Model           string          `json:"model"`
	Stream          bool            `json:"stream"`
	Input           json.RawMessage `json:"input"`
	Instructions    string          `json:"instructions"`
	MaxOutputTokens *int64          `json:"max_output_tokens,omitempty"`
	Temperature     *float64        `json:"temperature,omitempty"`
	TopP            *float64        `json:"top_p,omitempty"`
}

// responsesMessage is one input array entry.
type responsesMessage struct {
	Role    string          `json:"role"`
	Content json.RawMessage `json:"content"`
}

// ingestResponses parses a Responses request into the canonical form.
func ingestResponses(body []byte) (IngestResult, error) {
	var req responsesRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return IngestResult{}, fmt.Errorf("malformed responses request: %w", err)
	}
	if req.Model == "" {
		return IngestResult{}, fmt.Errorf("model is required")
	}

	chat := ChatRequest{
		Model:       req.Model,
		Stream:      req.Stream,
		MaxTokens:   req.MaxOutputTokens,
		Temperature: req.Temperature,
		TopP:        req.TopP,
	}

	// input: either a plain string or an array of role/content entries.
	if len(req.Input) > 0 {
		var text string
		if err := json.Unmarshal(req.Input, &text); err == nil {
			chat.Messages = append(chat.Messages, ChatMessage{Role: "user", Content: text})
		} else {
			var entries []responsesMessage
			if err := json.Unmarshal(req.Input, &entries); err != nil {
				return IngestResult{}, fmt.Errorf("input must be a string or a message array")
			}
			for _, entry := range entries {
				content, err := responsesText(entry.Content)
				if err != nil {
					return IngestResult{}, err
				}
				chat.Messages = append(chat.Messages, ChatMessage{Role: entry.Role, Content: content})
			}
		}
	}
	if req.Instructions != "" {
		// Instructions become the leading developer/system message.
		chat.Messages = append([]ChatMessage{{Role: "system", Content: req.Instructions}}, chat.Messages...)
	}

	upstream, err := json.Marshal(chat)
	if err != nil {
		return IngestResult{}, fmt.Errorf("encode canonical request: %w", err)
	}
	return IngestResult{Chat: chat, UpstreamBody: upstream}, nil
}

// responsesText extracts text from a content value: a plain string or
// an array of typed parts with input_text text.
func responsesText(raw json.RawMessage) (string, error) {
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return text, nil
	}
	var parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &parts); err != nil {
		return "", fmt.Errorf("content must be a string or a part array")
	}
	joined := ""
	for _, part := range parts {
		if part.Type != "input_text" && part.Type != "output_text" {
			return "", fmt.Errorf("unsupported input part type %q: text parts only", part.Type)
		}
		joined += part.Text
	}
	return joined, nil
}

// responsesWire renders the Responses surface.
type responsesWire struct{}

// Format implements Wire.
func (responsesWire) Format() Format { return FormatOpenAIResponses }

// RenderSuccess implements Wire: the upstream chat reply re-rendered as
// a completed Responses object.
func (responsesWire) RenderSuccess(w http.ResponseWriter, status int, header http.Header, upstreamBody []byte) {
	resp, ok := parseChatResponse(upstreamBody)
	if !ok {
		// Not a parseable chat reply: pass through rather than guess.
		renderExchangeBody(w, status, header, upstreamBody, passthroughHeaderNames)
		return
	}
	writeJSONResponse(w, status, responsesObject(resp.ID, resp.Model, resp.Choices[0].Message.Content, resp.Usage))
}

// RenderUpstreamError implements Wire: upstream errors are already
// OpenAI-shaped; rewrap under this wire's content type.
func (responsesWire) RenderUpstreamError(w http.ResponseWriter, status int, header http.Header, upstreamBody []byte) {
	renderExchangeBody(w, status, header, upstreamBody, passthroughHeaderNames)
}

// RenderError implements Wire.
func (responsesWire) RenderError(w http.ResponseWriter, status int, code, message string) {
	WriteError(w, status, code, message)
}

// Stream implements Wire.
func (responsesWire) Stream() StreamTranscoder { return &responsesStream{id: newGatewayID("resp_gw_")} }

// responsesObject builds the completed Responses object.
func responsesObject(id, model, text string, usage *Usage) map[string]any {
	obj := map[string]any{
		"id":     id,
		"object": "response",
		"status": "completed",
		"model":  model,
		"output": []map[string]any{{
			"type":   "message",
			"role":   "assistant",
			"status": "completed",
			"content": []map[string]any{{
				"type":        "output_text",
				"text":        text,
				"annotations": []any{},
			}},
		}},
	}
	if usage != nil {
		obj["usage"] = map[string]any{
			"input_tokens":  usage.PromptTokens,
			"output_tokens": usage.CompletionTokens,
			"total_tokens":  usage.TotalTokens,
		}
	}
	return obj
}

// responsesStream is the stateful Responses event translator.
type responsesStream struct {
	id    string
	model string
	text  []byte
}

// Start implements StreamTranscoder: response.created.
func (s *responsesStream) Start(w ioWriter, model string) error {
	s.model = model
	return WriteEvent(w, "response.created", map[string]any{
		"response": map[string]any{
			"id": s.id, "object": "response", "status": "in_progress", "model": model,
			"output": []any{},
		},
	})
}

// Delta implements StreamTranscoder: text pieces become
// response.output_text.delta frames; bookkeeping chunks are ignored.
func (s *responsesStream) Delta(w ioWriter, payload []byte) error {
	var chunk struct {
		Choices []struct {
			Delta struct {
				Content string `json:"content"`
			} `json:"delta"`
		} `json:"choices"`
	}
	if err := json.Unmarshal(payload, &chunk); err != nil || len(chunk.Choices) == 0 {
		return nil
	}
	if text := chunk.Choices[0].Delta.Content; text != "" {
		s.text = append(s.text, text...)
		return WriteEvent(w, "response.output_text.delta", map[string]any{
			"type":  "response.output_text.delta",
			"delta": text,
		})
	}
	return nil
}

// Finish implements StreamTranscoder: response.completed with the
// accumulated text and usage.
func (s *responsesStream) Finish(w ioWriter, usage Usage, usageKnown bool) error {
	completed := map[string]any{
		"response": map[string]any{
			"id": s.id, "object": "response", "status": "completed", "model": s.model,
			"output": []map[string]any{{
				"type":   "message",
				"role":   "assistant",
				"status": "completed",
				"content": []map[string]any{{
					"type":        "output_text",
					"text":        string(s.text),
					"annotations": []any{},
				}},
			}},
		},
	}
	if usageKnown {
		completed["response"].(map[string]any)["usage"] = map[string]any{
			"input_tokens":  usage.PromptTokens,
			"output_tokens": usage.CompletionTokens,
			"total_tokens":  usage.TotalTokens,
		}
	}
	return WriteEvent(w, "response.completed", completed)
}

// Abort implements StreamTranscoder: response.failed carries the
// frozen in-stream code, then the stream ends.
func (s *responsesStream) Abort(w ioWriter, code Code, message string) error {
	return WriteEvent(w, "response.failed", map[string]any{
		"response": map[string]any{
			"id": s.id, "object": "response", "status": "failed", "model": s.model,
			"error": map[string]any{
				"code":    string(code),
				"message": message,
			},
		},
	})
}

// newGatewayID generates a gateway-scoped object id; translated
// surfaces need ids before the upstream reveals its own.
func newGatewayID(prefix string) string {
	var buf [8]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return prefix + "0"
	}
	return prefix + hex.EncodeToString(buf[:])
}
