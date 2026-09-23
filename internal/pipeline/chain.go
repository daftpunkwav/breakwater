/**
 * @file chain
 * @description Ordered composition of pipeline stages over net/http handlers.
 *
 * Responsibilities:
 * - Compose governance stages into a single handler in a fixed order
 * - Nothing else: stage behavior lives in the owning modules
 *
 * Stages wrap http.Handler so that http.Flusher implementations survive
 * every layer, a prerequisite for SSE passthrough. A shared per-request
 * carrier joins when the first real stage lands.
 */
package pipeline

import "net/http"

// Middleware wraps a handler with one pipeline stage.
type Middleware func(http.Handler) http.Handler

// Chain composes middleware so the first listed stage runs outermost.
func Chain(stages ...Middleware) Middleware {
	return func(final http.Handler) http.Handler {
		for i := len(stages) - 1; i >= 0; i-- {
			final = stages[i](final)
		}
		return final
	}
}
