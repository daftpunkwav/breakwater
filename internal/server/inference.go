/**
 * @file inference
 * @description The business inference endpoints: the three client
 * formats (openai-chat, openai-responses, anthropic-messages), all
 * speaking to the same canonical pipeline.
 *
 * Responsibilities:
 * - Ensure the request body is ingested (canonical form + upstream
 *   bytes) in the route's format, enforcing model presence
 * - Hand the canonical exchange to the relay engine with the router's
 *   candidates and the format's wire
 * - Nothing else: every governance concern joins as pipeline
 *   middleware wrapped around this handler, and response rendering
 *   belongs to the relay and the wire
 */
package server

import (
	"net/http"

	"github.com/daftpunkwav/breakwater/internal/pipeline"
	"github.com/daftpunkwav/breakwater/internal/protocol"
	"github.com/daftpunkwav/breakwater/internal/relay"
	"github.com/daftpunkwav/breakwater/internal/router"
)

// routeOfFormat maps a client format to its route path.
func routeOfFormat(format protocol.Format) string {
	switch format {
	case protocol.FormatOpenAIResponses:
		return "POST /v1/responses"
	case protocol.FormatAnthropicMessages:
		return "POST /v1/messages"
	default:
		return "POST /v1/chat/completions"
	}
}

// inferenceServes the chat completion endpoints.
type Inference struct {
	format  protocol.Format
	router  router.Router
	relayer *relay.Executor
}

// NewInference builds the endpoint handler for one client format.
func NewInference(format protocol.Format, rt router.Router, relayer *relay.Executor) *Inference {
	return &Inference{format: format, router: rt, relayer: relayer}
}

// ServeHTTP implements http.Handler.
func (s *Inference) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	carrier := pipeline.CarrierFrom(r.Context())
	if carrier == nil {
		protocol.WireFor(s.format).RenderError(w, http.StatusInternalServerError, "pipeline_misconfigured",
			"no request carrier assembled")
		return
	}
	wire := protocol.WireFor(carrier.Format)
	if !pipeline.EnsureBody(w, r, carrier) {
		return
	}

	if carrier.Chat.Model == "" {
		wire.RenderError(w, http.StatusBadRequest, "invalid_request", "model is required")
		return
	}

	candidates, err := s.router.Candidates(r.Context(), carrier.Chat.Model)
	if err != nil {
		wire.RenderError(w, http.StatusNotFound, "model_not_found",
			"no upstream serves model "+carrier.Chat.Model)
		return
	}

	result := s.relayer.Execute(r.Context(), relay.Job{
		Model:      carrier.Chat.Model,
		Stream:     carrier.Chat.Stream,
		Body:       carrier.UpstreamBody,
		Candidates: candidates,
		Wire:       wire,
		Out:        w,
	})

	// Settlement input: real usage when the reply carried one, the
	// byte-derived estimate for streams that ended without it, the
	// reservation itself as the last resort for a delivered 2xx reply,
	// and zero for a failure — an error reply served no tokens, so its
	// reservation refunds in full.
	switch {
	case result.Status < 200 || result.Status > 299:
		carrier.Consumed = 0
	case result.UsageKnown:
		carrier.Consumed = result.Usage.TotalTokens
	case result.Streamed:
		carrier.Consumed = pipeline.EstimatePartialTokens(carrier.Chat, result.StreamBytes)
	default:
		carrier.Consumed = carrier.Tokens
	}
	carrier.Relay = &result
}
