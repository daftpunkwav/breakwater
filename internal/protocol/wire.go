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
// canonical form plus the forwardable body. The empty or unknown
// format parses as the canonical wire — the same contract WireFor
// holds, so a new format enum member missed in this switch surfaces as
// openai-chat behavior, never as a nil dereference.
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
// resolves to the canonical wire so callers never nil-check. That
// default is a contract, not an accident: adding a Format member means
// extending every switch over it (Ingest, WireFor, and the route table
// in internal/server) — until then the new format silently behaves as
// openai-chat, which the single formats slice at the composition root
// makes unlikely by registering every served surface in one place.
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
	renderExchangeBody(w, status, header, upstreamBody, PassthroughHeaderNames())
}

// RenderUpstreamError implements Wire: verbatim passthrough.
func (chatWire) RenderUpstreamError(w http.ResponseWriter, status int, header http.Header, upstreamBody []byte) {
	renderExchangeBody(w, status, header, upstreamBody, PassthroughHeaderNames())
}

// RenderError implements Wire: the OpenAI error envelope.
func (chatWire) RenderError(w http.ResponseWriter, status int, code, message string) {
	WriteError(w, status, code, message)
}

// Stream implements Wire: the canonical wire streams bytes untouched.
func (chatWire) Stream() StreamTranscoder { return nil }

// passthroughHeaderNames are the upstream response headers a client may
// act on and are therefore forwarded: the body's media type, and the
// Retry-After a 429/5xx owes its client. This is the single source of
// truth — the cache's replay of a stored entry forwards exactly this
// set, so a header added here reaches replayed responses too. The set
// is package-private; callers receive copies (PassthroughHeaderNames),
// so no call site can rewrite it for the whole process.
var passthroughHeaderNames = []string{"Content-Type", "Retry-After"}

// PassthroughHeaderNames returns the forwarded upstream response
// headers (see passthroughHeaderNames). The result is a fresh copy per
// call: a caller may treat it as mutable scratch without corrupting
// the shared set or racing a concurrent render reading it.
func PassthroughHeaderNames() []string {
	return append([]string(nil), passthroughHeaderNames...)
}

// ValidHeaderValue reports whether v is a legal HTTP header field
// value: visible ASCII, obs-text, space or horizontal tab (RFC 9110's
// field-value grammar). Every control byte — CR and LF included — and
// DEL disqualify it. The passthrough surfaces copy upstream-controlled
// values into the client response through this check, so a hostile or
// broken upstream cannot put control bytes on the wire (HTTP/1's
// space-replacement and HTTP/2's silent drop would otherwise give the
// two protocol versions different answers).
func ValidHeaderValue(v string) bool {
	for i := 0; i < len(v); i++ {
		if b := v[i]; (b < 0x20 && b != '\t') || b == 0x7f {
			return false
		}
	}
	return true
}

// renderExchangeBody writes status, selected headers and body. A
// broken upstream can report a status outside the renderable range;
// net/http panics on those, so the passthrough clamps instead. The
// selected headers carry upstream-controlled values, so each one is
// forwarded only when it is a legal header field value.
func renderExchangeBody(w http.ResponseWriter, status int, header http.Header, body []byte, names []string) {
	if status < 100 || status > 599 {
		status = http.StatusBadGateway
	}
	out := w.Header()
	for _, name := range names {
		if v := header.Get(name); v != "" && ValidHeaderValue(v) {
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
