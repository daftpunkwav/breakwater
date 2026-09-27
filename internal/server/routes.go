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

	"github.com/daftpunkwav/breakwater/internal/pipeline"
	"github.com/daftpunkwav/breakwater/internal/protocol"
)

// newRootHandler assembles the root handler with all routes registered.
// Inference maps each client format to its chain-wrapped handler; a
// missing format leaves that route unregistered. Readiness gates on
// the injected probe; nil always reports ready. Metrics and Admin
// expose their endpoints when non-nil. The model discovery route is
// registered only for a non-empty model list.
//
// The whole mux is wrapped in the recovery stage so the routes outside
// the inference chains — the admin surface, the probes, the metrics
// exposition — also return a rendered 500 rather than a connection
// killed by net/http's per-connection recover. Those routes carry no
// observation stage, so nothing is counted for them either way. The
// inference routes keep their own inner copy, which sits inside the
// observation stage: there a panic is counted as a 500 gateway fault.
func newRootHandler(inference map[protocol.Format]http.Handler, metrics, admin http.Handler, version string, readiness func() error, models []string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", handleLiveness)
	mux.HandleFunc("GET /version", makeVersion(version))
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
	if len(models) > 0 {
		mux.Handle("GET /v1/models", makeModelsHandler(models))
	}
	for format, handler := range inference {
		mux.Handle(routeOfFormat(format), handler)
	}
	return pipeline.RecoveryStage()(mux)
}
