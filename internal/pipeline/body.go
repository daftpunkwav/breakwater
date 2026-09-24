/**
 * @file body
 * @description The single body read shared by every stage that needs
 * the canonical request.
 *
 * Responsibilities:
 * - Read the bounded body exactly once per request, into the carrier,
 *   ingesting it through the wire of the carrier's format: the
 *   canonical chat request and the forwardable upstream bytes
 * - Render the transport-level rejections (too large, unreadable,
 *   malformed) in the client format's error envelope — they are
 *   request facts, not stage opinions
 * - Nothing else: eligibility, estimation and metering read the
 *   carrier's canonical copy afterwards
 */
package pipeline

import (
	"errors"
	"io"
	"net/http"

	"github.com/daftpunkwav/breakwater/internal/protocol"
)

// FormatStage pins the client-facing format of the route the chain
// serves; it must run before any stage that parses the body.
func FormatStage(format protocol.Format) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if carrier := CarrierFrom(r.Context()); carrier != nil {
				carrier.Format = format
			}
			next.ServeHTTP(w, r)
		})
	}
}

// EnsureBody makes the canonical request available on the carrier,
// reading and ingesting on first call. It renders and reports its own
// rejections in the client format; a false return means the response
// is already written.
func EnsureBody(w http.ResponseWriter, r *http.Request, carrier *Carrier) bool {
	if carrier == nil {
		protocol.WriteError(w, http.StatusInternalServerError, "pipeline_misconfigured",
			"no request carrier assembled")
		return false
	}
	wire := protocol.WireFor(carrier.Format)

	if carrier.ChatSet {
		if carrier.ChatErr != nil {
			wire.RenderError(w, http.StatusBadRequest, "invalid_request", carrier.ChatErr.Error())
			return false
		}
		return true
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, protocol.MaxBodyBytes))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			wire.RenderError(w, http.StatusRequestEntityTooLarge, "request_too_large",
				"request body exceeds the accepted size")
		} else {
			wire.RenderError(w, http.StatusBadRequest, "invalid_request",
				"unreadable request body")
		}
		return false
	}

	ingest, ingestErr := protocol.Ingest(carrier.Format, body)
	carrier.SetBody(body, ingest.Chat, ingest.UpstreamBody, ingestErr)
	if ingestErr != nil {
		wire.RenderError(w, http.StatusBadRequest, "invalid_request", ingestErr.Error())
		return false
	}
	return true
}
