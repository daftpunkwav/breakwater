/**
 * @file admin
 * @description The minimal management API: tenant quota balances
 * (read + top-up) and breaker states — exactly what operating the
 * gateway needs (PRD F9), never more.
 *
 * Responsibilities:
 * - Serve GET /admin/tenants/{id}/quota, PUT the same path (top-up or
 *   correction) and GET /admin/breakers
 * - Guard the surface with the configured admin token; an empty token
 *   disables authentication (local development only)
 * - Nothing else: the data comes from injected lookups; no governance
 *   decisions are made or changed here
 *
 * The PUT path is the only write on the surface. It exists because a
 * prepaid balance with no way to top up is a dead end: drained tenants
 * could never return.
 */
package server

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/daftpunkwav/breakwater/internal/circuit"
	"github.com/daftpunkwav/breakwater/internal/protocol"
	"github.com/daftpunkwav/breakwater/internal/quota"
)

// maxAdminBodyBytes bounds the body of admin writes; they carry one
// number at most.
const maxAdminBodyBytes = 1 << 12

// BalanceLookup reports a tenant's current quota balance. It returns
// quota.ErrUnknownTenant for a tenant with no ledger; any other error is
// a backend failure and renders as 503, never as a misleading 404.
type BalanceLookup func(r *http.Request, tenantID string) (int64, error)

// BalanceWriter provisions or resets a tenant balance (top-up,
// correction). It returns quota.ErrUnknownTenant for a tenant with no
// ledger.
type BalanceWriter func(r *http.Request, tenantID string, balance int64) error

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
	setter   BalanceWriter
	breakers BreakerStates
}

// NewAdmin builds the admin handler. A nil lookup or writer omits the
// corresponding endpoint.
func NewAdmin(token string, balances BalanceLookup, setter BalanceWriter, breakers BreakerStates) *Admin {
	return &Admin{token: token, balances: balances, setter: setter, breakers: breakers}
}

// ServeHTTP implements http.Handler: the bearer guard first, then the
// endpoints under method guards (GET reads, PUT tops up).
func (a *Admin) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !a.authorized(r) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	switch {
	case r.URL.Path == "/admin/breakers":
		a.guarded(w, r, http.MethodGet, a.serveBreakers)
	case strings.HasPrefix(r.URL.Path, "/admin/tenants/") && strings.HasSuffix(r.URL.Path, "/quota"):
		tenantID := strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/admin/tenants/"), "/quota")
		if tenantID == "" {
			http.NotFound(w, r)
			return
		}
		switch r.Method {
		case http.MethodGet:
			a.guarded(w, r, http.MethodGet, func(w http.ResponseWriter, r *http.Request) {
				a.serveQuota(w, r, tenantID)
			})
		case http.MethodPut:
			a.guarded(w, r, http.MethodPut, func(w http.ResponseWriter, r *http.Request) {
				a.serveQuotaTopUp(w, r, tenantID)
			})
		default:
			w.Header().Set("Allow", http.MethodGet+", "+http.MethodPut)
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	default:
		http.NotFound(w, r)
	}
}

// guarded runs a method-checked endpoint.
func (a *Admin) guarded(w http.ResponseWriter, r *http.Request, method string, fn http.HandlerFunc) {
	if r.Method != method {
		w.Header().Set("Allow", method)
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	fn(w, r)
}

// authorized checks the bearer token; empty token disables the check.
// The comparison is constant-time: the token guards a privileged
// surface and must not leak through timing.
func (a *Admin) authorized(r *http.Request) bool {
	if a.token == "" {
		return true
	}
	const prefix = "Bearer "
	raw := r.Header.Get("Authorization")
	if !strings.HasPrefix(raw, prefix) {
		return false
	}
	presented := strings.TrimSpace(raw[len(prefix):])
	return subtle.ConstantTimeCompare([]byte(presented), []byte(a.token)) == 1
}

func (a *Admin) serveQuota(w http.ResponseWriter, r *http.Request, tenantID string) {
	if a.balances == nil {
		http.NotFound(w, r)
		return
	}
	balance, err := a.balances(r, tenantID)
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, map[string]any{"tenant": tenantID, "balance": balance})
	case errors.Is(err, quota.ErrUnknownTenant):
		protocol.WriteError(w, http.StatusNotFound, "tenant_unknown", "no balance for tenant "+tenantID)
	default:
		// A ledger outage must not masquerade as an unknown tenant.
		protocol.WriteError(w, http.StatusServiceUnavailable, "balance_unavailable",
			"quota ledger unavailable")
	}
}

// serveQuotaTopUp handles PUT /admin/tenants/{id}/quota: the body is
// {"balance": N} and becomes the tenant's balance verbatim.
func (a *Admin) serveQuotaTopUp(w http.ResponseWriter, r *http.Request, tenantID string) {
	if a.setter == nil {
		http.NotFound(w, r)
		return
	}
	var body struct {
		Balance *int64 `json:"balance"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, maxAdminBodyBytes)).Decode(&body); err != nil || body.Balance == nil {
		protocol.WriteError(w, http.StatusBadRequest, "invalid_request",
			"body must be {\"balance\": <non-negative integer>}")
		return
	}
	if *body.Balance < 0 {
		protocol.WriteError(w, http.StatusBadRequest, "invalid_request",
			"balance must not be negative")
		return
	}

	err := a.setter(r, tenantID, *body.Balance)
	switch {
	case err == nil:
		writeJSON(w, http.StatusOK, map[string]any{"tenant": tenantID, "balance": *body.Balance})
	case errors.Is(err, quota.ErrUnknownTenant):
		protocol.WriteError(w, http.StatusNotFound, "tenant_unknown", "no balance for tenant "+tenantID)
	default:
		protocol.WriteError(w, http.StatusServiceUnavailable, "balance_unavailable",
			"quota ledger unavailable")
	}
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
