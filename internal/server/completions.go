/**
 * @file completions
 * @description The /v1/chat/completions endpoint: OpenAI-compatible
 * entry point of the gateway.
 *
 * Responsibilities:
 * - Accept and shallow-decode the request body (route on model, choose
 *   the transport mode), enforcing the body size cap
 * - Hand the request to the relay engine with the router's candidates
 * - Nothing else: every governance concern — auth, limiting, quota,
 *   caching — joins as pipeline middleware wrapped around this handler,
 *   and response rendering belongs to the relay
 */
package server

import (
	"errors"
	"io"
	"net/http"

	"github.com/daftpunkwav/breakwater/internal/pipeline"
	"github.com/daftpunkwav/breakwater/internal/protocol"
	"github.com/daftpunkwav/breakwater/internal/relay"
	"github.com/daftpunkwav/breakwater/internal/router"
)

// Completions serves the chat completion endpoint.
type Completions struct {
	router  router.Router
	relayer *relay.Executor
}

// NewCompletions builds the endpoint handler.
func NewCompletions(rt router.Router, relayer *relay.Executor) *Completions {
	return &Completions{router: rt, relayer: relayer}
}

// ServeHTTP implements http.Handler.
func (c *Completions) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	carrier := pipeline.CarrierFrom(r.Context())

	var body []byte
	var parsed protocol.ChatRequest
	if carrier != nil && carrier.ChatSet {
		// A governance stage already read and parsed the body.
		if carrier.ChatErr != nil {
			protocol.WriteError(w, http.StatusBadRequest, "invalid_request", "malformed JSON body")
			return
		}
		body, parsed = carrier.Body, carrier.Chat
	} else {
		var err error
		body, err = io.ReadAll(http.MaxBytesReader(w, r.Body, protocol.MaxBodyBytes))
		if err != nil {
			var tooLarge *http.MaxBytesError
			if errors.As(err, &tooLarge) {
				protocol.WriteError(w, http.StatusRequestEntityTooLarge, "request_too_large",
					"request body exceeds the accepted size")
				return
			}
			protocol.WriteError(w, http.StatusBadRequest, "invalid_request", "unreadable request body")
			return
		}
		parsed, err = protocol.ParseChatRequest(body)
		if err != nil {
			protocol.WriteError(w, http.StatusBadRequest, "invalid_request", "malformed JSON body")
			return
		}
		if carrier != nil {
			carrier.SetBody(body, parsed, nil)
		}
	}

	if parsed.Model == "" {
		protocol.WriteError(w, http.StatusBadRequest, "invalid_request", "model is required")
		return
	}

	candidates, err := c.router.Candidates(r.Context(), parsed.Model)
	if err != nil {
		protocol.WriteError(w, http.StatusNotFound, "model_not_found",
			"no upstream serves model "+parsed.Model)
		return
	}

	result := c.relayer.Execute(r.Context(), relay.Job{
		Model:      parsed.Model,
		Stream:     parsed.Stream,
		Body:       body,
		Candidates: candidates,
		Out:        w,
	})

	if carrier != nil {
		// Settlement input: real usage when the reply carried one, the
		// byte-derived estimate for streams that ended without it, and
		// the reservation itself as the last resort.
		switch {
		case result.UsageKnown:
			carrier.Consumed = result.Usage.TotalTokens
		case result.Streamed:
			carrier.Consumed = pipeline.EstimatePartialTokens(parsed, result.StreamBytes)
		default:
			carrier.Consumed = carrier.Tokens
		}
		carrier.Relay = &result
	}
}
