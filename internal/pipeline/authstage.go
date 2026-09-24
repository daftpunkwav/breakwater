/**
 * @file authstage
 * @description The authentication pipeline stage: API key to tenant.
 *
 * Responsibilities:
 * - Extract the bearer key, resolve the tenant through the auth store
 *   and attach it to the request carrier
 * - Reject unknown or malformed keys with 401 before anything
 *   downstream runs
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

// bearerPrefix is the accepted Authorization scheme.
const bearerPrefix = "Bearer "

// AuthStage returns the authentication stage.
func AuthStage(store auth.Store) Middleware {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			raw := r.Header.Get("Authorization")
			if !strings.HasPrefix(raw, bearerPrefix) {
				protocol.WriteError(w, http.StatusUnauthorized, "missing_api_key",
					"expected an Authorization: Bearer <key> header")
				return
			}
			key := strings.TrimSpace(strings.TrimPrefix(raw, bearerPrefix))
			if key == "" {
				protocol.WriteError(w, http.StatusUnauthorized, "missing_api_key",
					"the bearer credentials are empty")
				return
			}

			tenant, err := store.Resolve(r.Context(), key)
			switch {
			case errors.Is(err, auth.ErrUnauthorized):
				protocol.WriteError(w, http.StatusUnauthorized, "invalid_api_key",
					"unknown or revoked api key")
				return
			case err != nil:
				protocol.WriteError(w, http.StatusServiceUnavailable, "identity_unavailable",
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
