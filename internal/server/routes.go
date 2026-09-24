/**
 * @file routes
 * @description Route registration for the gateway HTTP surface.
 *
 * Responsibilities:
 * - Map paths to handlers in one place
 * - Keep the public URL layout explicit
 */
package server

import "net/http"

// newRootHandler assembles the root handler with all routes registered.
// A nil completions handler leaves the business route unregistered —
// the gateway then serves probes only. Readiness gates on the injected
// probe; nil always reports ready.
func newRootHandler(completions http.Handler, readiness func() error) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handleLiveness)
	mux.HandleFunc("GET /readyz", makeReadiness(readiness))
	if completions != nil {
		mux.Handle("POST /v1/chat/completions", completions)
	}
	return mux
}
