/**
 * @file upstream
 * @description The upstream port implemented by every provider adapter.
 *
 * Responsibilities:
 * - Define the single surface governance layers see: one forwarding
 *   exchange per call and the health probe
 * - Nothing else: protocol differences (payload translation, provider
 *   quirks) belong to each adapter; response body decoding (SSE or
 *   buffered) and usage extraction stay with the caller (the relay)
 *
 * Adding a provider means adding an implementation of this port; the
 * governance layers must not change (frozen architectural decision).
 *
 * Result convention (frozen):
 * - Forward reports every completed HTTP exchange — including 4xx and
 *   5xx responses — as a non-nil Response. A non-nil error means the
 *   exchange did not complete (connection failure, timeout, context
 *   cancellation).
 * - Callers classify retryability from the pair (retry layer) and
 *   health from the exchange alone (circuit layer); the port does not
 *   classify on their behalf.
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
	// Body is the neutral, OpenAI-format request body. Translating it to
	// the provider's wire format is the adapter's job, so failover can
	// re-translate the same body for a different provider.
	Body []byte
}

// Response is an upstream reply. Body is streamed and must be closed by
// the caller; for SSE responses the caller consumes it chunk by chunk.
// Header must be treated as read-only by callers; implementations that
// reuse internal buffers must return a private copy.
type Response struct {
	StatusCode int
	Header     http.Header
	Body       io.ReadCloser
}

// Upstream is the provider port.
type Upstream interface {
	// ID identifies the upstream instance for routing and breaker state.
	ID() string
	// Forward performs exactly one attempt under the result convention
	// in the file header; attempt timeout and cancellation are owned by
	// the retry layer through ctx.
	Forward(ctx context.Context, req Request) (*Response, error)
	// Probe reports whether the upstream currently accepts traffic.
	Probe(ctx context.Context) error
}
