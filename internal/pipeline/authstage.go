/**
 * @file authstage
 * @description The authentication pipeline stage: API key to tenant.
 *
 * Responsibilities:
 * - Extract the key (Authorization: Bearer, or x-api-key for
 *   Anthropic-style clients), resolve the tenant through the auth
 *   store and attach it to the request carrier
 * - Reject unknown or malformed keys with 401 in the client format
 *   before anything downstream runs
 * - Nothing else: limits and balances live downstream
 *
 * Placement note: this stage binds the carrier (defined in this
 * package) to the auth port, so it lives with the carrier instead of
 * the auth package — the reverse would make package auth depend on the
 * pipeline, while the carrier already depends on auth.Tenant. The
 * mechanism itself (stores, caching) stays entirely in package auth.
 *
 * A system-of-record outage surfaces as 503, never as a silent pass:
 * an unauthenticated request must not ride through on a store failure.
 */
package pipeline

import (
	"errors"
	"net/http"
	"strings"

	"github.com/daftpunkwav/breakwater/internal/auth"
	"github.com/daftpunkwav/breakwater/internal/protocol"
)

// apiKeyOf extracts the API key: Authorization: Bearer first, then the
// x-api-key header Anthropic clients send.
func apiKeyOf(r *http.Request) (string, bool) {
	if raw := r.Header.Get("Authorization"); strings.HasPrefix(raw, bearerPrefix) {
		if key := strings.TrimSpace(strings.TrimPrefix(raw, bearerPrefix)); key != "" {
			return key, true
		}
		return "", false
	}
	if key := strings.TrimSpace(r.Header.Get("x-api-key")); key != "" {
		return key, true
	}
	return "", false
}

// renderFor renders a governance rejection in the request's format; an
// absent carrier falls back to the canonical envelope.
func renderFor(r *http.Request, w http.ResponseWriter, status int, code, message string) {
	format := protocol.Format("")
	if carrier := CarrierFrom(r.Context()); carrier != nil {
		format = carrier.Format
	}
	protocol.WireFor(format).RenderError(w, status, code, message)
}

// bearerPrefix is the accepted Authorization scheme.
const bearerPrefix = "Bearer "

// AuthStage returns the authentication stage.
func AuthStage(store auth.Store) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key, ok := apiKeyOf(r)
			if !ok {
				renderFor(r, w, http.StatusUnauthorized, "missing_api_key",
					"expected an Authorization: Bearer <key> or x-api-key header")
				return
			}

			tenant, err := store.Resolve(r.Context(), key)
			switch {
			case errors.Is(err, auth.ErrUnauthorized):
				renderFor(r, w, http.StatusUnauthorized, "invalid_api_key",
					"unknown or revoked api key")
				return
			case err != nil:
				renderFor(r, w, http.StatusServiceUnavailable, "identity_unavailable",
					"identity store unavailable")
				return
			}

			carrier := CarrierFrom(r.Context())
			if carrier == nil {
				protocol.WriteError(w, http.StatusInternalServerError, "pipeline_misconfigured",
					"no request carrier assembled")
				return
			}
			carrier.Tenant = tenant
			next.ServeHTTP(w, r)
		})
	}
}
