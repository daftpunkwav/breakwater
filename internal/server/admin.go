/**
 * @file admin
 * @description The minimal management API: tenant quota balances and
 * breaker states — read only, exactly what operating the gateway needs
 * (PRD F9), never more.
 *
 * Responsibilities:
 * - Serve GET /admin/tenants/{id}/quota and GET /admin/breakers
 * - Guard the surface with the configured admin token; an empty token
 *   disables authentication (local development only)
 * - Nothing else: the data comes from injected lookups; no governance
 *   decisions are made or changed here
 */
package server

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/daftpunkwav/breakwater/internal/circuit"
	"github.com/daftpunkwav/breakwater/internal/protocol"
)

// BalanceLookup reports a tenant's current quota balance.
type BalanceLookup func(r *http.Request, tenantID string) (int64, error)

// BreakerStates lists the current breaker state of every configured
// upstream.
type BreakerStates func(r *http.Request) []BreakerView

// BreakerView is one breaker's wire form.
type BreakerView struct {
	Upstream string        `json:"upstream"`
	State    circuit.State `json:"state"`
}

// Admin serves the management endpoints.
type Admin struct {
	token    string
	balances BalanceLookup
	breakers BreakerStates
}

// NewAdmin builds the admin handler. A nil lookup omits the
// corresponding endpoint.
func NewAdmin(token string, balances BalanceLookup, breakers BreakerStates) *Admin {
	return &Admin{token: token, balances: balances, breakers: breakers}
}

// ServeHTTP implements http.Handler with method guards.
func (a *Admin) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !a.authorized(r) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	switch {
	case r.URL.Path == "/admin/breakers":
		a.serveBreakers(w, r)
	case strings.HasPrefix(r.URL.Path, "/admin/tenants/") && strings.HasSuffix(r.URL.Path, "/quota"):
		tenantID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/admin/tenants/"), "/quota")
		if tenantID == "" {
			http.NotFound(w, r)
			return
		}
		a.serveQuota(w, r, tenantID)
	default:
		http.NotFound(w, r)
	}
}

// authorized checks the bearer token; empty token disables the check.
func (a *Admin) authorized(r *http.Request) bool {
	if a.token == "" {
		return true
	}
	const prefix = "Bearer "
	if !strings.HasPrefix(r.Header.Get("Authorization"), prefix) {
		return false
	}
	return strings.TrimSpace(r.Header.Get("Authorization")[len(prefix):]) == a.token
}

func (a *Admin) serveQuota(w http.ResponseWriter, r *http.Request, tenantID string) {
	if a.balances == nil {
		http.NotFound(w, r)
		return
	}
	balance, err := a.balances(r, tenantID)
	if err != nil {
		protocol.WriteError(w, http.StatusNotFound, "tenant_unknown", "no balance for tenant "+tenantID)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"tenant": tenantID, "balance": balance})
}

func (a *Admin) serveBreakers(w http.ResponseWriter, r *http.Request) {
	if a.breakers == nil {
		http.NotFound(w, r)
		return
	}
	states := a.breakers(r)
	if states == nil {
		states = []BreakerView{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"breakers": states})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
