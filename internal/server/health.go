/**
 * @file health
 * @description Liveness and readiness probes.
 *
 * Responsibilities:
 * - Liveness: the process is up and able to serve
 * - Readiness: the gateway may receive business traffic
 *
 * Dependency probes join readiness as those integrations land, and they
 * must reflect dependencies whose absence flips the degradation posture:
 * while rate limiting is fail-closed, readiness has to gate on Redis
 * health or the probe lies. This file stays free of dependency-specific
 * code; probes are injected, not imported.
 */
package server

import "net/http"

func handleLiveness(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// makeReadiness binds the injected probe. While the limiter is
// fail-closed, readiness must gate on the dependency health or the
// probe would lie.
func makeReadiness(probe func() error) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		if probe != nil {
			if err := probe(); err != nil {
				w.Header().Set("Content-Type", "text/plain; charset=utf-8")
				w.WriteHeader(http.StatusServiceUnavailable)
				_, _ = w.Write([]byte("not ready: " + err.Error() + "\n"))
				return
			}
		}
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	}
}
