/**
 * @file body
 * @description The single body read shared by every stage that needs
 * the parsed request subset.
 *
 * Responsibilities:
 * - Read the bounded body exactly once per request, into the carrier
 * - Render the transport-level rejections (too large, unreadable,
 *   malformed JSON) — they are request facts, not stage opinions
 * - Nothing else: eligibility, estimation and metering read the
 *   carrier's parsed copy afterwards
 */
package pipeline

import (
	"errors"
	"io"
	"net/http"

	"github.com/daftpunkwav/breakwater/internal/protocol"
)

// EnsureBody makes the parsed request available on the carrier,
// reading and parsing on first call. It renders and reports its own
// rejections; a false return means the response is already written.
func EnsureBody(w http.ResponseWriter, r *http.Request, carrier *Carrier) bool {
	if carrier == nil {
		protocol.WriteError(w, http.StatusInternalServerError, "pipeline_misconfigured",
			"no request carrier assembled")
		return false
	}
	if carrier.ChatSet {
		if carrier.ChatErr != nil {
			protocol.WriteError(w, http.StatusBadRequest, "invalid_request", "malformed JSON body")
			return false
		}
		return true
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, protocol.MaxBodyBytes))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			protocol.WriteError(w, http.StatusRequestEntityTooLarge, "request_too_large",
				"request body exceeds the accepted size")
		} else {
			protocol.WriteError(w, http.StatusBadRequest, "invalid_request",
				"unreadable request body")
		}
		return false
	}
	parsed, parseErr := protocol.ParseChatRequest(body)
	carrier.SetBody(body, parsed, parseErr)
	if parseErr != nil {
		protocol.WriteError(w, http.StatusBadRequest, "invalid_request", "malformed JSON body")
		return false
	}
	return true
}
