/**
 * @file chain
 * @description Ordered composition of pipeline stages over net/http handlers.
 *
 * Responsibilities:
 * - Compose the request-side governance stages into a single handler in
 *   a fixed order
 * - Nothing else: stage behavior lives in the owning modules
 *
 * Two chains exist in the gateway; only the request side is composed
 * here:
 *
 *	request side:  carrier -> request id -> format -> observation
 *	               -> auth -> model authorization -> concurrency
 *	               -> limiter -> quota(reserve) -> cache -> route -> forward
 *	response side: forward -> retry/circuit -> quota(settle) -> cache write -> obs
 *
 * Retry, circuit breaking and failover wrap the upstream call INSIDE
 * the terminal forward stage; they must not be composed as Middleware
 * layers, because a retry cannot replay a half-consumed http.Handler
 * response. The terminal stage is owned by the relay engine
 * (internal/relay); stages that reserve before next() settle after it
 * returns (quota settles within its own middleware scope).
 *
 * Stages wrap http.Handler so that http.Flusher implementations survive
 * every layer, a prerequisite for SSE passthrough. The shared
 * per-request carrier is a typed struct assembled at chain entry, not
 * scattered context values.
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
