/**
 * @file carrier
 * @description The per-request state shared by pipeline stages.
 *
 * Responsibilities:
 * - Carry identity, the parsed request subset, the token estimate and
 *   the consumed-token handle from the stage that produces them to the
 *   stages that consume them
 * - Nothing else: stages own their behavior; the carrier is a dumb
 *   typed struct, never a grab-bag of context values
 *
 * Lifecycle: assembled once at chain entry, each field written by the
 * one stage that owns it. All access happens on the single request
 * goroutine, so no locking.
 */
package pipeline

import (
	"context"
	"net/http"

	"github.com/daftpunkwav/breakwater/internal/auth"
	"github.com/daftpunkwav/breakwater/internal/obs"
	"github.com/daftpunkwav/breakwater/internal/protocol"
)

// Response origins a request can be served from without an upstream
// touching it. They ride Carrier.ServedBy, never Forward.UpstreamID: the
// forward outcome's upstream field keeps naming real upstreams only, and
// the observation stage prefers ServedBy when it is set.
const (
	// SourceCache marks a response replayed from the cache store.
	SourceCache = "cache"
	// SourceSharedFetch marks a response a singleflight waiter replayed
	// from an in-flight fetch it did not start.
	SourceSharedFetch = "shared-fetch"
)

// Carrier is the per-request pipeline state.
type Carrier struct {
	// Tenant is the authenticated identity; zero until the auth stage
	// passed.
	Tenant auth.Tenant
	// RequestID identifies this request across response headers, the
	// access log and the upstream exchange; set by the request-ID stage
	// at chain entry.
	RequestID string
	// Format is the client-facing API surface of this request; the
	// format stage sets it before any body parsing.
	Format protocol.Format
	// Body is the raw request body as received; the first stage that
	// needs it reads it once, here.
	Body []byte
	// Chat is the canonical request subset; ChatErr carries the parse
	// failure for stages that care. ChatSet reports that the body was
	// already read, so later stages never re-read it.
	Chat    protocol.ChatRequest
	ChatErr error
	ChatSet bool
	// UpstreamBody is the bytes to forward upstream: the raw body for
	// the canonical wire, the canonical encoding for translated wires.
	UpstreamBody []byte
	// Tokens is the estimated, clamp-adjusted token cost every
	// reservation downstream is based on.
	Tokens int64
	// promptTokens caches the prompt-only token estimate: one scan of
	// the messages shared by every consumer (the limiter's reservation,
	// the context pre-filter, the partial-stream meter) instead of one
	// scan per consumer. Written once, on first use.
	promptTokens int64
	promptKnown  bool
	// Consumed is the tokens the request actually used; stages that
	// reject or bypass the upstream leave it at zero, and the refund
	// stages correct every reservation against it. SettleConsumed is
	// its single writer after the forward: the cache stage pre-sets it
	// to zero on the paths that never reach the upstream.
	Consumed int64
	// CacheHit reports a response served from the cache (a hit replay
	// or a shared singleflight fetch), for observation and refund.
	CacheHit bool
	// ServedBy names a response origin that is not an upstream: a
	// cache replay (SourceCache) or a shared fetch the request waited
	// on (SourceSharedFetch). Empty when the reply came from — or
	// failed at — a real upstream; that upstream's identity then rides
	// Forward.UpstreamID alone.
	ServedBy string
	// RejectCode records the rejection code of the stage that refused
	// the request, for the failure taxonomy. The governance stages
	// stamp the closed vocabulary defined in package protocol
	// (schema.go), each the same constant it renders the client
	// envelope with, so the carrier and the wire cannot drift; the
	// inference handler stamps the code its own routing decisions
	// render (model_not_found, model_disabled, circuit_open,
	// context_window_exceeded) for the same reason. Empty unless the
	// request was rejected before the forward stage.
	RejectCode string
	// Forward captures the forward stage's outcome for the stages after
	// it (settlement details, cache eligibility, observation
	// dimensions). It holds a pipeline-owned projection, not the relay
	// engine's own result type: the inference handler that runs the
	// forward call converts once at the boundary, so this contract and
	// every governance stage reading it stay free of the relay's shape
	// while relay never imports pipeline — the dependency runs one
	// way, from the governance chain to the execution engine it
	// invokes.
	Forward *Outcome
}

// SettleConsumed records how many tokens the completed exchange
// consumed against the carrier's reservation, reading the forward
// outcome the inference handler projected:
//
//   - real usage when the reply carried a usable accounting answer,
//   - the byte-derived estimate for a stream that ended without one,
//   - the reservation itself as the last resort for a delivered 2xx
//     reply,
//   - zero for a failure — an error reply served no tokens, so its
//     reservation refunds in full.
//
// A usage object reporting nothing spent (or less than nothing) is no
// accounting answer: the gateway cannot tell a provider's own cache
// hit from a reply it simply failed to meter. The zero rung is
// reserved for requests that served nothing — a rejection, a cache
// hit, a failed exchange — and a delivered 2xx landing there hands the
// tenant its whole reservation back for tokens the upstream really
// charged. The cache stage pre-sets Consumed to zero on its own paths
// (a replay, a shared fetch) and the terminal handler never runs
// there; a forward that never ran leaves Consumed untouched for the
// same reason.
func (c *Carrier) SettleConsumed() {
	if c.Forward == nil {
		return
	}
	switch {
	case c.Forward.Status < 200 || c.Forward.Status > 299:
		c.Consumed = 0
	case c.Forward.UsageKnown && c.Forward.Usage.TotalTokens > 0:
		c.Consumed = c.Forward.Usage.TotalTokens
	case c.Forward.Streamed:
		c.Consumed = c.EstimatePartialTokens(c.Forward.StreamBytes)
	default:
		c.Consumed = c.Tokens
	}
}

// Outcome is the forward stage's projected result: what the stages
// after the forward (settlement, cache write, observation) need from
// the exchange, in the pipeline's own vocabulary. The inference
// handler projects the relay engine's result into it at the single
// assignment point, so a relay field change stops at the server
// package and never re-shapes the carrier contract the governance
// packages read.
type Outcome struct {
	// UpstreamID names the upstream that produced the delivered reply;
	// empty when no upstream did.
	UpstreamID string
	// Status is the client status the forward stage sent (or intended,
	// when the client was already gone).
	Status int
	// ErrorCode is the gateway-originated failure code of a
	// gateway-rendered error (the closed vocabulary of package
	// protocol); empty for upstream error passthroughs, whose status
	// classifies them.
	ErrorCode string
	// Aborted reports a stream terminated honestly through the error
	// event contract after bytes had already reached the client: its
	// bytes are never a replayable completion.
	Aborted bool
	// ClientGone reports that the client disconnected before the
	// response completed.
	ClientGone bool
	// Streamed reports whether a streaming passthrough started;
	// StreamBytes is the byte count it delivered — the fallback
	// metering input when the stream ended without a usage report.
	Streamed    bool
	StreamBytes int64
	// Usage carries the token usage extracted from the reply;
	// UsageKnown is false when the reply carried none (settlement then
	// falls back to estimation).
	Usage      protocol.Usage
	UsageKnown bool
	// Trail is the per-attempt record of the forward walk, in the
	// access log's own shape: one entry per upstream attempt, in
	// order. The projection allocates it fresh per request, and the
	// observation stage hands it to the async sink by reference —
	// nothing may mutate it in place once projected.
	Trail []obs.AttemptTrace
}

// SetBody stores the request body and its ingest result exactly once;
// later calls are absorbed. The first reader owns the read — the body
// cap, format parsing and canonical encoding are request facts, not
// stage opinions.
func (c *Carrier) SetBody(body []byte, chat protocol.ChatRequest, upstreamBody []byte, ingestErr error) {
	if c.ChatSet {
		return
	}
	c.ChatSet = true
	c.Body = body
	c.Chat = chat
	c.UpstreamBody = upstreamBody
	c.ChatErr = ingestErr
}

// carrierKey is the unexported context key type.
type carrierKey struct{}

// WithCarrier attaches the carrier to the request context.
func WithCarrier(ctx context.Context, c *Carrier) context.Context {
	return context.WithValue(ctx, carrierKey{}, c)
}

// CarrierFrom returns the request carrier, or nil when no chain stage
// assembled one.
func CarrierFrom(ctx context.Context) *Carrier {
	c, _ := ctx.Value(carrierKey{}).(*Carrier)
	return c
}

// RequireCarrier returns the request carrier, rendering the
// pipeline_misconfigured envelope when the chain-entry stage did not
// assemble one; a false return means the response was already written.
// Every governance stage starts with this check.
func RequireCarrier(w http.ResponseWriter, r *http.Request) (*Carrier, bool) {
	carrier := CarrierFrom(r.Context())
	if carrier == nil {
		protocol.WriteError(w, http.StatusInternalServerError, string(protocol.CodePipelineMisconfigured),
			"no request carrier assembled")
		return nil, false
	}
	return carrier, true
}

// CarrierStage is the chain-entry middleware: it assembles the
// per-request carrier every governance stage shares.
func CarrierStage() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next.ServeHTTP(w, r.WithContext(WithCarrier(r.Context(), &Carrier{})))
		})
	}
}
