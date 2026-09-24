/**
 * @file wire
 * @description The client-facing protocol wires: how one canonical
 * OpenAI-chat exchange is presented to clients of each ingress format.
 *
 * Responsibilities:
 * - Define the translation contract every ingress format implements:
 *   request ingest into the canonical form, response rendering out of
 *   the upstream's OpenAI-chat result, error envelopes, and streaming
 *   frame translation
 * - Nothing else: governance and routing see only the canonical form;
 *   format differences never leak past this package's boundary
 *
 * Organization follows the translator-matrix discipline (one file per
 * format, one test file per format): openai-chat is the canonical wire
 * itself (byte passthrough, nothing lost), openai-responses and
 * anthropic-messages translate through the canonical form — request
 * fields they cannot express are rejected loudly, upstream fields they
 * cannot express are dropped honestly.
 */
package protocol

import (
	"encoding/json"
	"net/http"
)

// Format names one client-facing API surface.
type Format string

const (
	// FormatOpenAIChat is the canonical wire: POST /v1/chat/completions.
	FormatOpenAIChat Format = "openai-chat"
	// FormatOpenAIResponses: POST /v1/responses.
	FormatOpenAIResponses Format = "openai-responses"
	// FormatAnthropicMessages: POST /v1/messages.
	FormatAnthropicMessages Format = "anthropic-messages"
)

// IngestResult is the outcome of parsing one client request.
type IngestResult struct {
	// Chat is the canonical request every governance layer reads
	// (model, stream mode, token estimate inputs, sampling params).
	Chat ChatRequest
	// UpstreamBody is the bytes to forward: the raw request for the
	// canonical wire (unknown fields preserved), the canonical encoding
	// for translated wires.
	UpstreamBody []byte
}

// Ingest parses a client request of the given format into the
// canonical form plus the forwardable body.
func Ingest(format Format, body []byte) (IngestResult, error) {
	switch format {
	case FormatOpenAIResponses:
		return ingestResponses(body)
	case FormatAnthropicMessages:
		return ingestAnthropic(body)
	default:
		chat, err := ParseChatRequest(body)
		if err != nil {
			return IngestResult{}, err
		}
		return IngestResult{Chat: chat, UpstreamBody: body}, nil
	}
}

// Wire adapts the canonical exchange to one client-facing format. The
// implementations are stateless singletons; per-request state lives in
// the StreamTranscoder a Wire hands out.
type Wire interface {
	// Format names the client surface.
	Format() Format
	// RenderSuccess writes a completed non-streaming response,
	// translating the upstream OpenAI-chat body when the format
	// differs.
	RenderSuccess(w http.ResponseWriter, status int, header http.Header, upstreamBody []byte)
	// RenderUpstreamError passes a completed upstream error exchange
	// through in the client's format.
	RenderUpstreamError(w http.ResponseWriter, status int, header http.Header, upstreamBody []byte)
	// RenderError writes a gateway-originated failure in the format's
	// error envelope.
	RenderError(w http.ResponseWriter, status int, code, message string)
	// Stream returns the stateful stream translator for streamed
	// exchanges; nil means the format is the canonical wire and SSE
	// bytes pass through untouched.
	Stream() StreamTranscoder
}

// StreamTranscoder translates the upstream OpenAI-chat SSE sequence
// into the client format's event stream. One instance per request; its
// methods are called from the single pump goroutine in order: Start,
// Delta per upstream data frame, then exactly one of Finish or Abort.
type StreamTranscoder interface {
	// Start writes the stream preamble (message_start /
	// response.created) after the response headers flushed. Object ids
	// are gateway-generated inside the transcoder: upstream ids are not
	// known until later frames.
	Start(w ioWriter, model string) error
	// Delta receives one upstream data-line JSON payload (a chat
	// completion chunk, [DONE] excluded) and emits translated frames.
	Delta(w ioWriter, payload []byte) error
	// Finish writes the success terminator, carrying the final usage
	// when the upstream reported one.
	Finish(w ioWriter, usage Usage, usageKnown bool) error
	// Abort writes the honest failure terminator after bytes already
	// reached the client; the stream ends after it returns.
	Abort(w ioWriter, code Code, message string) error
}

// ioWriter keeps the transcoder signatures free of an os import.
type ioWriter = interface {
	Write(p []byte) (int, error)
}

// WireFor returns the wire of a format; the empty or unknown format
// resolves to the canonical wire so callers never nil-check.
func WireFor(format Format) Wire {
	switch format {
	case FormatOpenAIResponses:
		return responsesWire{}
	case FormatAnthropicMessages:
		return anthropicWire{}
	default:
		return chatWire{}
	}
}

// chatWire is the identity: the canonical wire itself.
type chatWire struct{}

// Format implements Wire.
func (chatWire) Format() Format { return FormatOpenAIChat }

// RenderSuccess implements Wire: verbatim passthrough.
func (chatWire) RenderSuccess(w http.ResponseWriter, status int, header http.Header, upstreamBody []byte) {
	renderExchangeBody(w, status, header, upstreamBody, passthroughHeaderNames)
}

// RenderUpstreamError implements Wire: verbatim passthrough.
func (chatWire) RenderUpstreamError(w http.ResponseWriter, status int, header http.Header, upstreamBody []byte) {
	renderExchangeBody(w, status, header, upstreamBody, passthroughHeaderNames)
}

// RenderError implements Wire: the OpenAI error envelope.
func (chatWire) RenderError(w http.ResponseWriter, status int, code, message string) {
	WriteError(w, status, code, message)
}

// Stream implements Wire: the canonical wire streams bytes untouched.
func (chatWire) Stream() StreamTranscoder { return nil }

// passthroughHeaders are the upstream response headers a client may
// act on and are therefore forwarded.
var passthroughHeaderNames = []string{"Content-Type", "Retry-After"}

// renderExchangeBody writes status, selected headers and body.
func renderExchangeBody(w http.ResponseWriter, status int, header http.Header, body []byte, names []string) {
	out := w.Header()
	for _, name := range names {
		if v := header.Get(name); v != "" {
			out.Set(name, v)
		}
	}
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// writeJSONResponse writes one JSON body with the content type set.
func writeJSONResponse(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

// chatResponse is the upstream non-streaming chat completion reply,
// parsed to extract what translated wires re-render.
type chatResponse struct {
	ID      string `json:"id"`
	Model   string `json:"model"`
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
		FinishReason string `json:"finish_reason"`
	} `json:"choices"`
	Usage *Usage `json:"usage"`
}

// parseChatResponse decodes the upstream reply; ok is false when the
// body is not a parseable chat completion.
func parseChatResponse(body []byte) (chatResponse, bool) {
	var resp chatResponse
	if err := json.Unmarshal(body, &resp); err != nil || len(resp.Choices) == 0 {
		return chatResponse{}, false
	}
	return resp, true
}

// upstreamErrorBody is the error payload of an upstream OpenAI error
// envelope, extracted when translated wires re-render it.
type upstreamErrorBody struct {
	Error struct {
		Message string `json:"message"`
		Code    string `json:"code"`
	} `json:"error"`
}

// parseUpstreamError extracts message and code from an upstream error
// body; missing fields fall back to honest placeholders.
func parseUpstreamError(body []byte) (message, code string) {
	var parsed upstreamErrorBody
	_ = json.Unmarshal(body, &parsed)
	if parsed.Error.Message == "" {
		parsed.Error.Message = "upstream request failed"
	}
	if parsed.Error.Code == "" {
		parsed.Error.Code = "upstream_error"
	}
	return parsed.Error.Message, parsed.Error.Code
}
