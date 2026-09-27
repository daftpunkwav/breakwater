/**
 * @file recovery
 * @description The panic containment stage: a handler panic becomes a
 * rendered 500 instead of a killed connection.
 *
 * Responsibilities:
 * - Recover a panic from the chain's innermost handler, render the
 *   client's error envelope when the response has not started, and log
 *   the stack loudly
 * - Nothing else: recovery is containment, not classification — the
 *   observation stage records the rendered 500 as the gateway fault it
 *   is, so a panic surfaces in the success rate instead of masquerading
 *   as a client disconnect
 *
 * Placement is innermost, immediately around the endpoint handler: the
 * first handler code sits at the chain's end, and that is where panics
 * come from. A committed response (streaming past the first byte)
 * cannot take a second header; recovery then only logs — the client
 * sees the truncation it would have seen without this stage.
 */
package pipeline

import (
	"log/slog"
	"net/http"
	"runtime/debug"

	"github.com/daftpunkwav/breakwater/internal/protocol"
)

// RecoveryStage returns the panic containment stage.
func RecoveryStage() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				rec := recover()
				if rec == nil {
					return
				}
				slog.Error("handler panic recovered",
					"panic", rec, "method", r.Method, "path", r.URL.Path,
					"stack", string(debug.Stack()))
				if t, ok := w.(interface{ Status() int }); ok && t.Status() != 0 {
					// Headers already committed: no second response is
					// possible. The truncated stream is what the client
					// gets; the stack lives in the log.
					return
				}
				format := protocol.FormatOpenAIChat
				if carrier := CarrierFrom(r.Context()); carrier != nil {
					format = carrier.Format
				}
				protocol.WireFor(format).RenderError(w, http.StatusInternalServerError,
					"internal_error", "the gateway hit an internal error")
			}()
			next.ServeHTTP(w, r)
		})
	}
}
