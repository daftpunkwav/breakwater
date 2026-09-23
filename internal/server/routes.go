/**
 * @file routes
 * @description Route registration for the gateway HTTP surface.
 *
 * Responsibilities:
 * - Map paths to handlers in one place
 * - Keep the public URL layout explicit
 *
 * The /v1/chat/completions route and the pipeline it fronts are added by
 * the forwarding milestone; governance stages must not be wired here.
 */
package server

import "net/http"

// newRootHandler assembles the root handler with all routes registered.
func newRootHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handleLiveness)
	mux.HandleFunc("GET /readyz", handleReadiness)
	return mux
}
