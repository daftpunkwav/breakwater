/**
 * @file upstream
 * @description The upstream port implemented by every provider adapter.
 *
 * Responsibilities:
 * - Define the single surface governance layers see: forwarding, health
 *   probing and identity
 * - Nothing else: protocol differences (payload normalization, usage
 *   extraction) belong to each adapter
 *
 * Adding a provider means adding an implementation of this port; the
 * governance layers must not change (frozen architectural decision).
 * The forwarding surface will be refined when streaming semantics are
 * implemented and must not grow provider-specific members.
 */
package upstream

import (
	"context"
	"io"
	"net/http"
)

// Request is one forwarded chat completion attempt.
type Request struct {
	// Model is the model identifier requested by the client.
	Model string
	// Stream reports whether a streaming (SSE) response is expected.
	Stream bool
	// Payload is the request body, already normalized for the provider.
	Payload []byte
}

// Response is an upstream reply. Body is streamed and must be closed by
// the caller; for SSE responses the caller consumes it chunk by chunk.
type Response struct {
	StatusCode int
	Header     http.Header
	Body       io.ReadCloser
}

// Upstream is the provider port.
type Upstream interface {
	// ID identifies the upstream instance for routing and breaker state.
	ID() string
	// Forward performs exactly one attempt; attempt timeout and
	// cancellation are owned by the retry layer through ctx.
	Forward(ctx context.Context, req Request) (*Response, error)
	// Probe reports whether the upstream currently accepts traffic.
	Probe(ctx context.Context) error
}
