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
	"context"
	"errors"
	"net/http"

	"github.com/daftpunkwav/breakwater/internal/pipeline"
	"github.com/daftpunkwav/breakwater/internal/protocol"
	"github.com/daftpunkwav/breakwater/internal/relay"
	"github.com/daftpunkwav/breakwater/internal/router"
	"github.com/daftpunkwav/breakwater/internal/upstream"
)

// overContext reports whether the model declares an input ceiling and
// the prompt exceeds it — the one condition the pre-filter refuses.
// Models without a ceiling are unlimited; the estimate fails open.
func overContext(limits map[string]int64, model string, inputTokens int64) bool {
	limit, ok := limits[model]
	return ok && limit > 0 && inputTokens > limit
}

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

// Inference serves one client format's inference endpoint.
type Inference struct {
	format  protocol.Format
	router  router.Router
	relayer *relay.Executor
	// fallbacks maps the client-facing model to its ordered fallback
	// models; a nil map or a missing model means no chain.
	fallbacks map[string][]string
	// contextLimits maps the client-facing model to its maximum input
	// token estimate; a model whose ceiling the request cannot fit
	// loses every candidate up front.
	contextLimits map[string]int64
}

// InferenceOption customizes an Inference.
type InferenceOption func(*Inference)

// WithFallbacks installs the per-model fallback chains; nil (the
// default) disables them.
func WithFallbacks(m map[string][]string) InferenceOption {
	return func(s *Inference) { s.fallbacks = m }
}

// WithContextLimits installs the per-model input ceilings; nil (the
// default) disables the pre-filter.
func WithContextLimits(m map[string]int64) InferenceOption {
	return func(s *Inference) { s.contextLimits = m }
}

// NewInference builds the endpoint handler for one client format.
func NewInference(format protocol.Format, rt router.Router, relayer *relay.Executor, opts ...InferenceOption) *Inference {
	s := &Inference{format: format, router: rt, relayer: relayer}
	for _, opt := range opts {
		opt(s)
	}
	return s
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
	// Tier model authorization — defense in depth: the pipeline's authz
	// stage applies the same rule earlier (before any governance spend),
	// and this is the fail-closed last line for the ungoverned mode (no
	// identity store, no stage installed). AuthorizeModel is the one
	// authority both call, so the verdicts cannot drift.
	if !pipeline.AuthorizeModel(w, r, carrier) {
		return
	}

	candidates, err := s.router.Candidates(r.Context(), carrier.Chat.Model)
	if err != nil {
		if errors.Is(err, router.ErrDisabled) {
			// An operator switched the model off: a deliberate refusal,
			// distinct from a configuration gap (404) and a health
			// condition (503).
			wire.RenderError(w, http.StatusForbidden, "model_disabled",
				"model "+carrier.Chat.Model+" is disabled by the operator")
			return
		}
		if errors.Is(err, router.ErrUnavailable) {
			// The model exists but every binding is ineligible: the
			// same fast-fail contract the executor renders at attempt
			// time, so the condition reads identically wherever it is
			// detected.
			wire.RenderError(w, http.StatusServiceUnavailable, "circuit_open",
				"every upstream serving model "+carrier.Chat.Model+" is unavailable")
			return
		}
		wire.RenderError(w, http.StatusNotFound, "model_not_found",
			"no upstream serves model "+carrier.Chat.Model)
		return
	}

	// Context-window pre-filter: a model whose declared ceiling cannot
	// hold the prompt loses every candidate before the relay spends an
	// attempt on a doomed exchange. The estimate is approximate by
	// design, so the filter fails open — models without a ceiling are
	// never filtered.
	inputTokens := pipeline.PromptTokens(carrier.Chat)
	if overContext(s.contextLimits, carrier.Chat.Model, inputTokens) {
		wire.RenderError(w, http.StatusRequestEntityTooLarge, "context_window_exceeded",
			"prompt does not fit any candidate model for "+carrier.Chat.Model)
		return
	}

	// The fallback chain and its resolver: the relay walks the chain
	// only when the primary model's candidates are exhausted. The
	// resolver applies every gate the primary passed — tier
	// authorization first (a fallback must never serve a model the
	// tenant is not entitled to, deny winning per the tier contract),
	// then the context filter.
	var chain []string
	if len(s.fallbacks) > 0 {
		chain = s.fallbacks[carrier.Chat.Model]
	}
	var resolve relay.CandidateResolver
	if len(chain) > 0 {
		limits, rt, tenant := s.contextLimits, s.router, carrier.Tenant
		resolve = func(ctx context.Context, model string) ([]upstream.Upstream, error) {
			if tenant.ID != "" && !tenant.Tier.AllowsModel(model) {
				return nil, router.ErrUnavailable
			}
			if overContext(limits, model, inputTokens) {
				return nil, router.ErrUnavailable
			}
			return rt.Candidates(ctx, model)
		}
	}

	result := s.relayer.Execute(r.Context(), relay.Job{
		Model:      carrier.Chat.Model,
		Stream:     carrier.Chat.Stream,
		Body:       carrier.UpstreamBody,
		Candidates: candidates,
		Fallbacks:  chain,
		Resolve:    resolve,
		Wire:       wire,
		RequestID:  carrier.RequestID,
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
