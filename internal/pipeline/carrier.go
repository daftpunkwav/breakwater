/**
 * @file carrier
 * @description The per-request state shared by pipeline stages.
 *
 * Responsibilities:
 * - Carry identity, the parsed request subset, the token estimate and
 *   the governance handles (lease, consumed tokens) from the stage that
 *   produces them to the stages that consume them
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
	"github.com/daftpunkwav/breakwater/internal/protocol"
	"github.com/daftpunkwav/breakwater/internal/relay"
)

// Carrier is the per-request pipeline state.
type Carrier struct {
	// Tenant is the authenticated identity; zero until the auth stage
	// passed.
	Tenant auth.Tenant
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
	// Lease is the quota lease reserved for this request.
	Lease string
	// Consumed is the tokens the request actually used; stages that
	// reject or bypass the upstream leave it at zero, and the refund
	// stages correct every reservation against it.
	Consumed int64
	// CacheHit reports a response served from the cache (a hit replay
	// or a shared singleflight fetch), for observation and refund.
	CacheHit bool
	// Relay captures the forward stage's outcome for the stages after
	// it (settlement details, observation dimensions).
	Relay *relay.Result
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
		protocol.WriteError(w, http.StatusInternalServerError, "pipeline_misconfigured",
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
