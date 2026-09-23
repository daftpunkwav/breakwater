/**
 * @file health
 * @description Liveness and readiness probes.
 *
 * Responsibilities:
 * - Liveness: the process is up and able to serve
 * - Readiness: the gateway may receive business traffic
 *
 * Dependency probes (Redis, PostgreSQL) join readiness as those
 * integrations land; this file must stay free of dependency-specific code.
 */
package server

import "net/http"

func handleLiveness(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func handleReadiness(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}
