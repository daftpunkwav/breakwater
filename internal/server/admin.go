/**
 * @file admin
 * @description The minimal management API: tenant quota balances
 * (read + top-up), breaker states and the runtime traffic switches
 * over models and upstreams — exactly what operating the gateway
 * needs (PRD F9), never more.
 *
 * Responsibilities:
 * - Own the surface's shared machinery: the bearer guard, the route
 *   dispatch, the strict body decoder and the JSON writer
 * - Serve GET /admin/tenants/{id}/quota, PUT the same path (top-up or
 *   correction), GET /admin/breakers, GET /admin/routing and PUT
 *   /admin/models/{id} and /admin/upstreams/{id} (enable/disable)
 * - Guard the surface with the configured admin token; an empty token
 *   disables authentication (local development only)
 * - Nothing else: quota and breaker data come from injected lookups;
 *   the switches are the one mutable control object, and the only
 *   place a live request path and the admin surface meet; the identity
 *   management endpoints live in identityadmin.go
 *
 * The PUT paths are the surface's writes. They exist because prepaid
 * balances with no way to top up are a dead end, and because routing
 * changes (disable a misbehaving model or provider) must not require
 * a restart.
 */
package server

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/daftpunkwav/breakwater/internal/auth"
	"github.com/daftpunkwav/breakwater/internal/circuit"
	"github.com/daftpunkwav/breakwater/internal/protocol"
	"github.com/daftpunkwav/breakwater/internal/quota"
	"github.com/daftpunkwav/breakwater/internal/router"
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
	routing  *router.Switch
	identity auth.AdminStore
}

// AdminOption customizes an Admin.
type AdminOption func(*Admin)

// WithRouting installs the runtime traffic switches; nil (the default)
// omits the routing endpoints.
func WithRouting(s *router.Switch) AdminOption {
	return func(a *Admin) { a.routing = s }
}

// WithIdentityStore installs the identity administration port; nil
// (the default) omits the user and key management endpoints — the
// static identity mode has no administration surface.
func WithIdentityStore(s auth.AdminStore) AdminOption {
	return func(a *Admin) { a.identity = s }
}

// NewAdmin builds the admin handler. A nil lookup or writer omits the
// corresponding endpoint.
func NewAdmin(token string, balances BalanceLookup, setter BalanceWriter, breakers BreakerStates, opts ...AdminOption) *Admin {
	a := &Admin{token: token, balances: balances, setter: setter, breakers: breakers}
	for _, opt := range opts {
		opt(a)
	}
	return a
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
	case r.URL.Path == "/admin/routing":
		a.guarded(w, r, http.MethodGet, a.serveRouting)
	case r.URL.Path == "/admin/users" || strings.HasPrefix(r.URL.Path, "/admin/users/") ||
		strings.HasPrefix(r.URL.Path, "/admin/keys/"):
		a.serveIdentity(w, r)
	case strings.HasPrefix(r.URL.Path, "/admin/models/"):
		a.guarded(w, r, http.MethodPut, func(w http.ResponseWriter, r *http.Request) {
			a.serveModelSwitch(w, r, strings.TrimPrefix(r.URL.Path, "/admin/models/"))
		})
	case strings.HasPrefix(r.URL.Path, "/admin/upstreams/"):
		a.guarded(w, r, http.MethodPut, func(w http.ResponseWriter, r *http.Request) {
			a.serveUpstreamSwitch(w, r, strings.TrimPrefix(r.URL.Path, "/admin/upstreams/"))
		})
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
	if !decodeAdminJSON(w, r, &body) {
		return
	}
	if body.Balance == nil {
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

// serveRouting handles GET /admin/routing: every known model and
// upstream with its current eligibility.
func (a *Admin) serveRouting(w http.ResponseWriter, r *http.Request) {
	if a.routing == nil {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, http.StatusOK, a.routing.View())
}

// serveModelSwitch handles PUT /admin/models/{id}: the body is
// {"enabled": false} and takes the model out of routing immediately.
func (a *Admin) serveModelSwitch(w http.ResponseWriter, r *http.Request, model string) {
	if a.routing == nil || model == "" {
		http.NotFound(w, r)
		return
	}
	enabled, ok := decodeEnabled(w, r)
	if !ok {
		return
	}
	if err := a.routing.SetModel(model, enabled); err != nil {
		if errors.Is(err, router.ErrUnknownModel) {
			protocol.WriteError(w, http.StatusNotFound, "model_unknown", err.Error())
			return
		}
		protocol.WriteError(w, http.StatusInternalServerError, "routing_switch_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"model": model, "enabled": enabled})
}

// serveUpstreamSwitch handles PUT /admin/upstreams/{id}: the body is
// {"enabled": false} and drops the upstream from every candidate list.
func (a *Admin) serveUpstreamSwitch(w http.ResponseWriter, r *http.Request, id string) {
	if a.routing == nil || id == "" {
		http.NotFound(w, r)
		return
	}
	enabled, ok := decodeEnabled(w, r)
	if !ok {
		return
	}
	if err := a.routing.SetUpstream(id, enabled); err != nil {
		if errors.Is(err, router.ErrUnknownUpstream) {
			protocol.WriteError(w, http.StatusNotFound, "upstream_unknown", err.Error())
			return
		}
		protocol.WriteError(w, http.StatusInternalServerError, "routing_switch_failed", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"upstream": id, "enabled": enabled})
}

// decodeEnabled parses the {"enabled": bool} switch payload shared by
// /admin/models, /admin/upstreams and /admin/keys/{id}/status. A
// typo'd extra member is rejected like on every other governance
// write, never silently ignored.
func decodeEnabled(w http.ResponseWriter, r *http.Request) (bool, bool) {
	var body struct {
		Enabled *bool `json:"enabled"`
	}
	if !decodeAdminJSON(w, r, &body) {
		return false, false
	}
	if body.Enabled == nil {
		protocol.WriteError(w, http.StatusBadRequest, "invalid_request",
			"body must be {\"enabled\": true|false}")
		return false, false
	}
	return *body.Enabled, true
}

// decodeAdminJSON parses one admin write body within the size cap.
// Unknown fields are rejected: a typo'd member (say "rpms" for "rpm")
// must fail the write loudly, never land as a silent no-op on a
// governance surface.
func decodeAdminJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(io.LimitReader(r.Body, maxAdminBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		protocol.WriteError(w, http.StatusBadRequest, "invalid_request",
			"malformed or unexpected request body")
		return false
	}
	return true
}

// decodeAdminJSONOptional is decodeAdminJSON for the endpoints whose
// payload is optional (the unnamed-key issuance): an absent body (EOF)
// decodes as the zero value, every other malformed or unknown-member
// document is rejected exactly the same way.
func decodeAdminJSONOptional(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(io.LimitReader(r.Body, maxAdminBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil && !errors.Is(err, io.EOF) {
		protocol.WriteError(w, http.StatusBadRequest, "invalid_request",
			"malformed or unexpected request body")
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
