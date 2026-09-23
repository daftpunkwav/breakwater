/**
 * @file health
 * @description Health probe of the mock upstream.
 *
 * Responsibilities:
 * - Report that the mock process is serving, so compose health checks
 *   and experiment scripts have a stable probe target
 */
package mockllm

import "net/http"

// handleHealth answers the liveness probe.
func handleHealth(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}
