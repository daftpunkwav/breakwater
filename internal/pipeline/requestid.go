/**
 * @file requestid
 * @description The request-ID pipeline stage: one identifier that
 * follows the request everywhere the gateway touches it.
 *
 * Responsibilities:
 * - Adopt a client-supplied X-Request-Id when it is well-formed, or
 *   mint a fresh one; echo the result in the response header of every
 *   outcome, rejections included
 * - Record it on the carrier so the access log and the upstream
 *   exchange carry the same correlation key
 * - Nothing else: the ID is opaque plumbing, never parsed, never used
 *   for routing or authorization
 *
 * Why it exists (the Envoy/Kong practice, adapted to the evidence
 * story): "the client saw a 502" must be joinable with exactly one
 * access-log line and one upstream exchange when an incident is
 * reconstructed from the logs.
 */
package pipeline

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"regexp"
)

// RequestIDHeader is the header the gateway echoes upstream and to
// clients.
const RequestIDHeader = "X-Request-Id"

// requestIDPattern bounds adopted client-supplied ids: printable ASCII
// without spaces or control characters, 8..128 chars. Anything else is
// replaced, never trusted.
var requestIDPattern = regexp.MustCompile(`^[\x21-\x7e]{8,128}$`)

// requestIDPrefix marks gateway-minted ids.
const requestIDPrefix = "req-"

// RequestIDStage returns the correlation stage: adopt or mint the ID,
// echo it, attach it to the carrier. It runs directly after the
// carrier stage so every later stage and every rejection path carries
// the identifier.
func RequestIDStage() Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			id := adoptRequestID(r.Header.Get(RequestIDHeader))
			w.Header().Set(RequestIDHeader, id)
			if carrier := CarrierFrom(r.Context()); carrier != nil {
				carrier.RequestID = id
			}
			next.ServeHTTP(w, r)
		})
	}
}

// adoptRequestID returns a well-formed client-supplied id or mints a
// fresh gateway id.
func adoptRequestID(candidate string) string {
	if requestIDPattern.MatchString(candidate) {
		return candidate
	}
	return mintRequestID()
}

// mintRequestID generates a random, prefixed identifier.
func mintRequestID() string {
	var buf [12]byte
	if _, err := rand.Read(buf[:]); err != nil {
		// Entropy failure cannot produce a safe id; a fixed fallback
		// keeps the pipeline alive and stays visible in the logs.
		return requestIDPrefix + "entropy-fallback"
	}
	return requestIDPrefix + hex.EncodeToString(buf[:])
}
