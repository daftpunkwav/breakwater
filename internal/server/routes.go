/**
 * @file routes
 * @description Route registration for the gateway HTTP surface.
 *
 * Responsibilities:
 * - Map paths to handlers in one place
 * - Keep the public URL layout explicit
 */
package server

import (
	"net/http"

	"github.com/daftpunkwav/breakwater/internal/protocol"
)

// newRootHandler assembles the root handler with all routes registered.
// Inference maps each client format to its chain-wrapped handler; a
// missing format leaves that route unregistered. Readiness gates on
// the injected probe; nil always reports ready. Metrics and Admin
// expose their endpoints when non-nil.
func newRootHandler(inference map[protocol.Format]http.Handler, metrics, admin http.Handler, readiness func() error) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handleLiveness)
	mux.HandleFunc("GET /readyz", makeReadiness(readiness))
	if metrics != nil {
		mux.Handle("GET /metrics", metrics)
	}
	if admin != nil {
		// Deliberately not method-qualified: the admin handler guards
		// methods itself (GET reads, PUT tops up), while a "GET /admin/"
		// pattern would bounce PUT at the mux with a 405 the top-up
		// endpoint could never see.
		mux.Handle("/admin/", admin)
	}
	for format, handler := range inference {
		mux.Handle(routeOfFormat(format), handler)
	}
	return mux
}
