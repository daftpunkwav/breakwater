/**
 * @file schema
 * @description The subset of the OpenAI chat completion schema the
 * gateway consumes, plus the usage and error envelopes it renders.
 *
 * Responsibilities:
 * - Decode requests just deep enough to route (model), choose a
 *   transport mode (stream), meter (message sizes, max_tokens) and
 *   decide cache eligibility (sampling parameters)
 * - Nothing else: the body bytes are forwarded verbatim; unknown fields
 *   are never rejected and never re-encoded on the request side
 */
package protocol

import (
	"encoding/json"
	"net/http"
)

// MaxBodyBytes caps the accepted request body size of forwarded
// completions.
const MaxBodyBytes = 4 << 20

// ChatMessage is one conversation entry.
type ChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// StreamOptions mirrors the OpenAI stream options object. The usage
// chunk it requests is what settlement reads on streaming paths; the
// request body itself is always forwarded verbatim.
type StreamOptions struct {
	IncludeUsage bool `json:"include_usage,omitempty"`
}

// ChatRequest is the decoded subset of a chat completion request.
// Pointer fields distinguish "absent" from "zero", which both token
// estimation and cache eligibility rely on.
type ChatRequest struct {
	Model       string        `json:"model"`
	Stream      bool          `json:"stream"`
	Messages    []ChatMessage `json:"messages"`
	MaxTokens   *int64        `json:"max_tokens,omitempty"`
	Temperature *float64      `json:"temperature,omitempty"`
	TopP        *float64      `json:"top_p,omitempty"`
	N           *int          `json:"n,omitempty"`
	Seed        *int64        `json:"seed,omitempty"`
	// Stop, tools and every other pass-through field are intentionally
	// not modeled: the body is forwarded as received.
	StreamOptions *StreamOptions `json:"stream_options,omitempty"`
}

// Usage reports token accounting of one completion.
type Usage struct {
	PromptTokens     int64 `json:"prompt_tokens"`
	CompletionTokens int64 `json:"completion_tokens"`
	TotalTokens      int64 `json:"total_tokens"`
}

// ParseChatRequest decodes the request subset from a body. Unknown
// fields are ignored; the caller keeps forwarding the original bytes.
func ParseChatRequest(body []byte) (ChatRequest, error) {
	var req ChatRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return ChatRequest{}, err
	}
	return req, nil
}

// ParseUsage extracts the usage object from a completion response body
// (non-streaming JSON, or the final usage chunk of a stream). ok is
// false when the body is malformed or carries no usage — providers that
// omit usage exist, and settlement needs an explicit signal for that.
func ParseUsage(body []byte) (usage Usage, ok bool) {
	var envelope struct {
		Usage *Usage `json:"usage"`
	}
	if err := json.Unmarshal(body, &envelope); err != nil || envelope.Usage == nil {
		return Usage{}, false
	}
	return *envelope.Usage, true
}

// ErrorEnvelope is the OpenAI-style error response body the gateway
// renders for its own failures (upstream unreachable, no route, model
// unknown). Provider errors are passed through verbatim instead.
type ErrorEnvelope struct {
	Error ErrorBody `json:"error"`
}

// ErrorBody is the error payload of an ErrorEnvelope.
type ErrorBody struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    string `json:"code"`
}

// gatewayErrorType marks gateway-originated failures, both as the HTTP
// envelope type and as the in-stream error type of the frozen SSE
// contract.
const gatewayErrorType = "gateway_error"

// WriteError renders an error envelope with the given HTTP status.
func WriteError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(ErrorEnvelope{
		Error: ErrorBody{Message: message, Type: gatewayErrorType, Code: code},
	})
}
