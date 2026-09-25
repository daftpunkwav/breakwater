/**
 * @file admin
 * @description The minimal management API: tenant quota balances
 * (read + top-up), breaker states and the runtime traffic switches
 * over models and upstreams — exactly what operating the gateway
 * needs (PRD F9), never more.
 *
 * Responsibilities:
 * - Serve GET /admin/tenants/{id}/quota, PUT the same path (top-up or
 *   correction), GET /admin/breakers, GET /admin/routing and PUT
 *   /admin/models/{id} and /admin/upstreams/{id} (enable/disable)
 * - Guard the surface with the configured admin token; an empty token
 *   disables authentication (local development only)
 * - Nothing else: quota and breaker data come from injected lookups;
 *   the switches are the one mutable control object, and the only
 *   place a live request path and the admin surface meet
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
	case r.URL.Path == "/admin/users":
		if r.Method == http.MethodPost {
			a.guarded(w, r, http.MethodPost, a.serveCreateUser)
		} else {
			a.guarded(w, r, http.MethodGet, a.serveListUsers)
		}
	case strings.HasPrefix(r.URL.Path, "/admin/users/"):
		rest := strings.TrimPrefix(r.URL.Path, "/admin/users/")
		switch {
		case strings.HasSuffix(rest, "/limits"):
			id := strings.TrimSuffix(rest, "/limits")
			a.guarded(w, r, http.MethodPut, func(w http.ResponseWriter, r *http.Request) {
				a.serveUserLimits(w, r, id)
			})
		case strings.HasSuffix(rest, "/keys"):
			id := strings.TrimSuffix(rest, "/keys")
			if r.Method == http.MethodPost {
				a.guarded(w, r, http.MethodPost, func(w http.ResponseWriter, r *http.Request) {
					a.serveCreateKey(w, r, id)
				})
			} else {
				a.guarded(w, r, http.MethodGet, func(w http.ResponseWriter, r *http.Request) {
					a.serveListKeys(w, r, id)
				})
			}
		default:
			http.NotFound(w, r)
		}
	case strings.HasPrefix(r.URL.Path, "/admin/keys/"):
		rest := strings.TrimPrefix(r.URL.Path, "/admin/keys/")
		switch {
		case strings.HasSuffix(rest, "/limits"):
			id := strings.TrimSuffix(rest, "/limits")
			a.guarded(w, r, http.MethodPut, func(w http.ResponseWriter, r *http.Request) {
				a.serveKeyLimits(w, r, id)
			})
		case strings.HasSuffix(rest, "/status"):
			id := strings.TrimSuffix(rest, "/status")
			a.guarded(w, r, http.MethodPut, func(w http.ResponseWriter, r *http.Request) {
				a.serveKeyStatus(w, r, id)
			})
		default:
			http.NotFound(w, r)
		}
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

// decodeEnabled parses the {"enabled": bool} switch payload.
func decodeEnabled(w http.ResponseWriter, r *http.Request) (bool, bool) {
	var body struct {
		Enabled *bool `json:"enabled"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, maxAdminBodyBytes)).Decode(&body); err != nil || body.Enabled == nil {
		protocol.WriteError(w, http.StatusBadRequest, "invalid_request",
			"body must be {\"enabled\": true|false}")
		return false, false
	}
	return *body.Enabled, true
}

// serveCreateUser handles POST /admin/users.
func (a *Admin) serveCreateUser(w http.ResponseWriter, r *http.Request) {
	if a.identity == nil {
		http.NotFound(w, r)
		return
	}
	var body struct {
		Name string `json:"name"`
		Role string `json:"role"`
		Tier string `json:"tier"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, maxAdminBodyBytes)).Decode(&body); err != nil || body.Name == "" || body.Tier == "" {
		protocol.WriteError(w, http.StatusBadRequest, "invalid_request",
			`body must be {"name": string, "tier": string, "role": "user"|"admin" (default user)}`)
		return
	}
	role := auth.RoleUser
	if body.Role != "" && body.Role != string(auth.RoleUser) {
		role = auth.Role(body.Role)
	}
	id, err := a.identity.CreateUser(r.Context(), body.Name, role, body.Tier)
	if err != nil {
		a.renderIdentityError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"id": id, "name": body.Name, "role": string(role), "tier": body.Tier})
}

func (a *Admin) serveListUsers(w http.ResponseWriter, r *http.Request) {
	if a.identity == nil {
		http.NotFound(w, r)
		return
	}
	users, err := a.identity.Users(r.Context())
	if err != nil {
		a.renderIdentityError(w, err)
		return
	}
	if users == nil {
		users = []auth.UserView{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"users": users})
}

// serveUserLimits handles PUT /admin/users/{id}/limits: the body is a
// full LimitOverride document and replaces the user's layer. Every key
// of the user resolves through it; changes surface within the auth
// cache TTL.
func (a *Admin) serveUserLimits(w http.ResponseWriter, r *http.Request, userID string) {
	if a.identity == nil || userID == "" {
		http.NotFound(w, r)
		return
	}
	o, ok := decodeOverride(w, r)
	if !ok {
		return
	}
	if err := a.identity.SetUserLimits(r.Context(), userID, o); err != nil {
		a.renderIdentityError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"user": userID, "overrides": o})
}

// serveCreateKey handles POST /admin/users/{id}/keys. The raw key is
// returned exactly once — the store keeps only the hash.
func (a *Admin) serveCreateKey(w http.ResponseWriter, r *http.Request, userID string) {
	if a.identity == nil || userID == "" {
		http.NotFound(w, r)
		return
	}
	var body struct {
		Name string `json:"name"`
	}
	_ = json.NewDecoder(io.LimitReader(r.Body, maxAdminBodyBytes)).Decode(&body)
	issued, err := a.identity.CreateKey(r.Context(), userID, body.Name)
	if err != nil {
		a.renderIdentityError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, issued)
}

func (a *Admin) serveListKeys(w http.ResponseWriter, r *http.Request, userID string) {
	if a.identity == nil || userID == "" {
		http.NotFound(w, r)
		return
	}
	keys, err := a.identity.Keys(r.Context(), userID)
	if err != nil {
		a.renderIdentityError(w, err)
		return
	}
	if keys == nil {
		keys = []auth.KeyView{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"keys": keys})
}

// serveKeyLimits handles PUT /admin/keys/{id}/limits: the body is a
// full LimitOverride document for the key's layer.
func (a *Admin) serveKeyLimits(w http.ResponseWriter, r *http.Request, keyID string) {
	if a.identity == nil || keyID == "" {
		http.NotFound(w, r)
		return
	}
	o, ok := decodeOverride(w, r)
	if !ok {
		return
	}
	if err := a.identity.SetKeyLimits(r.Context(), keyID, o); err != nil {
		a.renderIdentityError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"key": keyID, "overrides": o})
}

func (a *Admin) serveKeyStatus(w http.ResponseWriter, r *http.Request, keyID string) {
	if a.identity == nil || keyID == "" {
		http.NotFound(w, r)
		return
	}
	active, ok := decodeEnabled(w, r)
	if !ok {
		return
	}
	if err := a.identity.SetKeyStatus(r.Context(), keyID, active); err != nil {
		a.renderIdentityError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"key": keyID, "active": active})
}

// decodeOverride parses a LimitOverride payload (an omitted field
// means "inherit"; the caller may also send {} to clear a layer).
func decodeOverride(w http.ResponseWriter, r *http.Request) (auth.LimitOverride, bool) {
	var o auth.LimitOverride
	if err := json.NewDecoder(io.LimitReader(r.Body, maxAdminBodyBytes)).Decode(&o); err != nil {
		protocol.WriteError(w, http.StatusBadRequest, "invalid_request",
			"body must be a limits override document")
		return auth.LimitOverride{}, false
	}
	return o, true
}

// renderIdentityError maps the admin store's sentinel errors onto the
// management surface's status codes.
func (a *Admin) renderIdentityError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, auth.ErrUnknownUser), errors.Is(err, auth.ErrUnknownKey),
		errors.Is(err, auth.ErrUnknownTier):
		protocol.WriteError(w, http.StatusNotFound, "identity_unknown", err.Error())
	case errors.Is(err, auth.ErrTooManyKeys):
		protocol.WriteError(w, http.StatusConflict, "key_limit_reached", err.Error())
	default:
		protocol.WriteError(w, http.StatusServiceUnavailable, "identity_store_unavailable",
			"identity store unavailable")
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
