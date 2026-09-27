/**
 * @file health
 * @description Liveness and readiness probes.
 *
 * Responsibilities:
 * - Liveness: the process is up and able to serve
 * - Readiness: the gateway may receive business traffic
 *
 * Readiness reflects the dependencies whose absence flips the
 * degradation posture: while rate limiting is fail-closed, readiness
 * has to gate on Redis health or the probe lies. This file stays free
 * of dependency-specific code; probes are injected, not imported.
 */
package server

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"runtime"
)

func handleLiveness(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// makeVersion renders the build identifier plus the running Go
// version — the first thing an incident report asks for.
func makeVersion(version string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"version": version,
			"go":      runtime.Version(),
		})
	}
}

// makeReadiness binds the injected probe. While the limiter is
// fail-closed, readiness must gate on the dependency health or the
// probe would lie. The endpoint is unauthenticated and dependency
// errors carry infrastructure details (addresses, database user and
// name — the exact things that must not leak during an outage), so the
// body stays a stable phrase and the detail goes to the log.
func makeReadiness(probe func() error) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if probe != nil {
			if err := probe(); err != nil {
				slog.Warn("readiness probe failed", "error", err)
				w.Header().Set("Content-Type", "text/plain; charset=utf-8")
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte("not ready\n"))
				return
			}
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	}
}
