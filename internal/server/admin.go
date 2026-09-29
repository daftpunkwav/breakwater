/**
 * @file admin
 * @description The minimal management API: tenant quota balances
 * (read + top-up), breaker states and the runtime traffic switches
 * over models and upstreams — exactly what operating the gateway
 * needs, never more.
 *
 * Responsibilities:
 * - Own the surface's shared machinery: the bearer guard, the route
 *   dispatch, the strict body decoder and the JSON writer
 * - Serve GET /admin/tenants/{id}/quota, PUT the same path (top-up or
 *   correction), GET /admin/breakers, POST /admin/breakers/{id}/reset,
 *   GET /admin/routing, GET /admin/insights, POST
 *   /admin/upstreams/{id}/probe and PUT /admin/models/{id} and
 *   /admin/upstreams/{id} (enable/disable)
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
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/daftpunkwav/breakwater/internal/auth"
	"github.com/daftpunkwav/breakwater/internal/circuit"
	"github.com/daftpunkwav/breakwater/internal/insights"
	"github.com/daftpunkwav/breakwater/internal/protocol"
	"github.com/daftpunkwav/breakwater/internal/quota"
	"github.com/daftpunkwav/breakwater/internal/router"
)

// maxAdminBodyBytes bounds the body of admin writes; they carry one
// number at most.
const maxAdminBodyBytes = 1 << 12

// validTenantID is the tenant identifier rule the quota endpoints
// enforce on the admin-supplied path segment: the same character set
// the identity stores accept (internal/auth static tenants) and the
// config layer enforces on upstream ids (internal/config). Both are
// separate regexes on purpose — this package must not import config,
// and auth's rule guards construction, not this surface's input — but
// the three must stay in lockstep: a tenant id is a Redis key segment,
// a ledger identity and an access-log field at once.
var validTenantID = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`).MatchString

// BalanceLookup reports a tenant's current quota balance. It returns
// quota.ErrUnknownTenant for a tenant with no ledger; any other error is
// a backend failure and renders as 503, never as a misleading 404.
type BalanceLookup func(r *http.Request, tenantID string) (int64, error)

// BalanceWriter provisions or resets a tenant balance (top-up,
// correction). A tenant with no ledger gets one — the write is an
// upsert, per the quota.Ledger SetBalance contract — so a PUT to an
// unprovisioned id creates the balance instead of failing; any returned
// error is a backend failure.
type BalanceWriter func(r *http.Request, tenantID string, balance int64) error

// BreakerStates lists the current breaker state of every configured
// upstream.
type BreakerStates func(r *http.Request) []BreakerView

// ErrUnknownUpstream reports a breaker-reset or probe call naming an
// upstream the gateway does not configure.
var ErrUnknownUpstream = errors.New("admin: unknown upstream")

// ErrProbeUnconfigured reports a probe call for an upstream that
// declares no probe_url: there is nothing to ask off the request path.
var ErrProbeUnconfigured = errors.New("admin: upstream declares no probe url")

// BreakerReset forces one upstream's breaker back to closed (an
// operator fix followed by "let it through now").
type BreakerReset func(r *http.Request, upstreamID string) error

// UpstreamProbe runs one on-demand health probe against an upstream.
type UpstreamProbe func(r *http.Request, upstreamID string) error

// BreakerView is one breaker's wire form.
type BreakerView struct {
	Upstream string        `json:"upstream"`
	State    circuit.State `json:"state"`
}

// Reporter is the assessment port the insights endpoint needs: one
// stability report for a window. *insights.PGStore implements it.
type Reporter interface {
	Report(ctx context.Context, from, to time.Time) (insights.Report, error)
}

// Admin serves the management endpoints.
type Admin struct {
	token         string
	balances      BalanceLookup
	setter        BalanceWriter
	breakers      BreakerStates
	breakerReset  BreakerReset
	upstreamProbe UpstreamProbe
	routing       *router.Switch
	identity      auth.AdminStore
	insights      Reporter
}

// AdminOption customizes an Admin.
type AdminOption func(*Admin)

// WithRouting installs the runtime traffic switches; nil (the default)
// omits the routing endpoints.
func WithRouting(s *router.Switch) AdminOption {
	return func(a *Admin) { a.routing = s }
}

// WithBreakerReset installs the breaker-reset action; nil (the default)
// omits the reset endpoint.
func WithBreakerReset(fn BreakerReset) AdminOption {
	return func(a *Admin) { a.breakerReset = fn }
}

// WithUpstreamProbe installs the on-demand probe action; nil (the
// default) omits the probe endpoint.
func WithUpstreamProbe(fn UpstreamProbe) AdminOption {
	return func(a *Admin) { a.upstreamProbe = fn }
}

// WithIdentityStore installs the identity administration port; nil
// (the default) omits the user and key management endpoints — the
// static identity mode has no administration surface.
func WithIdentityStore(s auth.AdminStore) AdminOption {
	return func(a *Admin) { a.identity = s }
}

// WithInsights installs the monitoring store backing the assessment
// endpoint; nil (the default) omits it.
func WithInsights(s Reporter) AdminOption {
	return func(a *Admin) { a.insights = s }
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
		// The same JSON envelope every other admin rejection uses, plus
		// the WWW-Authenticate challenge RFC 6750 expects on a 401 —
		// script clients read the status, humans read the body.
		w.Header().Set("WWW-Authenticate", `Bearer realm="breakwater-admin"`)
		protocol.WriteError(w, http.StatusUnauthorized, "unauthorized",
			"admin bearer token missing or invalid")
		return
	}
	switch {
	case r.URL.Path == "/admin/breakers":
		a.guarded(w, r, http.MethodGet, a.serveBreakers)
	case strings.HasPrefix(r.URL.Path, "/admin/breakers/") && strings.HasSuffix(r.URL.Path, "/reset"):
		a.guarded(w, r, http.MethodPost, func(w http.ResponseWriter, r *http.Request) {
			a.serveBreakerReset(w, r, strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/admin/breakers/"), "/reset"))
		})
	case r.URL.Path == "/admin/routing":
		a.guarded(w, r, http.MethodGet, a.serveRouting)
	case r.URL.Path == "/admin/insights":
		a.guarded(w, r, http.MethodGet, a.serveInsights)
	case r.URL.Path == "/admin/users" || strings.HasPrefix(r.URL.Path, "/admin/users/") ||
		strings.HasPrefix(r.URL.Path, "/admin/keys/"):
		a.serveIdentity(w, r)
	case strings.HasPrefix(r.URL.Path, "/admin/upstreams/") && strings.HasSuffix(r.URL.Path, "/probe"):
		a.guarded(w, r, http.MethodPost, func(w http.ResponseWriter, r *http.Request) {
			a.serveUpstreamProbe(w, r, strings.TrimSuffix(strings.TrimPrefix(r.URL.Path, "/admin/upstreams/"), "/probe"))
		})
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
		// The tenant id is a Redis/ledger key segment and an access-log
		// field downstream; a character outside the identity stores' own
		// rule would create or read ledger entries no real tenant can
		// ever own. Reject it here, at the surface's boundary, before the
		// ledger is consulted.
		if !validTenantID(tenantID) {
			protocol.WriteError(w, http.StatusBadRequest, "invalid_request",
				"tenant ids are 1-128 characters of [A-Za-z0-9._-]")
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
// The comparison is constant-time in both content and length:
// ConstantTimeCompare alone leaks a length mismatch through its early
// return, so fixed-length digests are compared instead — the token
// guards a privileged surface and must not leak through timing.
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
	given := sha256.Sum256([]byte(presented))
	want := sha256.Sum256([]byte(a.token))
	return subtle.ConstantTimeCompare(given[:], want[:]) == 1
}

// renderQuotaError maps the balance port's errors onto the quota
// endpoints' status codes — one mapping shared by the read and the
// top-up path, so the two cannot drift: an unknown tenant is a 404 (an
// upsert writer never reports it, but a stricter one maps like the read
// path), and any other failure is a 503 — a ledger outage must never
// masquerade as an unknown tenant.
func renderQuotaError(w http.ResponseWriter, tenantID string, err error) {
	if errors.Is(err, quota.ErrUnknownTenant) {
		protocol.WriteError(w, http.StatusNotFound, "tenant_unknown", "no balance for tenant "+tenantID)
		return
	}
	protocol.WriteError(w, http.StatusServiceUnavailable, "balance_unavailable",
		"quota ledger unavailable")
}

func (a *Admin) serveQuota(w http.ResponseWriter, r *http.Request, tenantID string) {
	if a.balances == nil {
		http.NotFound(w, r)
		return
	}
	balance, err := a.balances(r, tenantID)
	if err != nil {
		renderQuotaError(w, tenantID, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"tenant": tenantID, "balance": balance})
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
	if err != nil {
		renderQuotaError(w, tenantID, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"tenant": tenantID, "balance": *body.Balance})
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

// serveBreakerReset handles POST /admin/breakers/{id}/reset: the
// operator's "I fixed the upstream, let it through now". The
// machine's own cooldown-and-probe path stays the automatic route.
func (a *Admin) serveBreakerReset(w http.ResponseWriter, r *http.Request, id string) {
	if a.breakerReset == nil || id == "" {
		http.NotFound(w, r)
		return
	}
	switch err := a.breakerReset(r, id); {
	case err == nil:
		writeJSON(w, http.StatusOK, map[string]any{"upstream": id, "state": string(circuit.StateClosed)})
	case errors.Is(err, ErrUnknownUpstream):
		protocol.WriteError(w, http.StatusNotFound, "upstream_unknown", err.Error())
	default:
		protocol.WriteError(w, http.StatusInternalServerError, "breaker_reset_failed", err.Error())
	}
}

// serveUpstreamProbe handles POST /admin/upstreams/{id}/probe: one
// health exchange on demand, so an operator can check recovery without
// waiting for the recovery loop's next tick.
func (a *Admin) serveUpstreamProbe(w http.ResponseWriter, r *http.Request, id string) {
	if a.upstreamProbe == nil || id == "" {
		http.NotFound(w, r)
		return
	}
	switch err := a.upstreamProbe(r, id); {
	case err == nil:
		writeJSON(w, http.StatusOK, map[string]any{"upstream": id, "ok": true})
	case errors.Is(err, ErrUnknownUpstream):
		protocol.WriteError(w, http.StatusNotFound, "upstream_unknown", err.Error())
	case errors.Is(err, ErrProbeUnconfigured):
		protocol.WriteError(w, http.StatusConflict, "probe_unconfigured", err.Error())
	default:
		protocol.WriteError(w, http.StatusBadGateway, "probe_failed", err.Error())
	}
}

// serveInsights handles GET /admin/insights?hours=N: the stability
// report for the trailing window (default 24, capped at 30 days).
func (a *Admin) serveInsights(w http.ResponseWriter, r *http.Request) {
	if a.insights == nil {
		http.NotFound(w, r)
		return
	}
	hours := int64(24)
	if raw := r.URL.Query().Get("hours"); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed < 1 || parsed > 24*30 {
			protocol.WriteError(w, http.StatusBadRequest, "invalid_request",
				"hours must be an integer between 1 and 720")
			return
		}
		hours = parsed
	}
	to := time.Now().UTC()
	from := to.Add(-time.Duration(hours) * time.Hour)
	report, err := a.insights.Report(r.Context(), from, to)
	if err != nil {
		protocol.WriteError(w, http.StatusServiceUnavailable, "insights_unavailable",
			"monitoring store unavailable")
		return
	}
	writeJSON(w, http.StatusOK, report)
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
// governance surface. Trailing data after the first JSON value is
// rejected the same way: `{"balance":5} {"balance":9}` must never
// silently take the first document.
func decodeAdminJSON(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(io.LimitReader(r.Body, maxAdminBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		protocol.WriteError(w, http.StatusBadRequest, "invalid_request",
			"malformed or unexpected request body")
		return false
	}
	if err := dec.Decode(v); !errors.Is(err, io.EOF) {
		protocol.WriteError(w, http.StatusBadRequest, "invalid_request",
			"unexpected data after the JSON body")
		return false
	}
	return true
}

// decodeAdminJSONOptional is decodeAdminJSON for the endpoints whose
// payload is optional (the unnamed-key issuance): an absent body (EOF)
// decodes as the zero value, every other malformed or unknown-member
// document — trailing data included — is rejected exactly the same way.
func decodeAdminJSONOptional(w http.ResponseWriter, r *http.Request, v any) bool {
	dec := json.NewDecoder(io.LimitReader(r.Body, maxAdminBodyBytes))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil && !errors.Is(err, io.EOF) {
		protocol.WriteError(w, http.StatusBadRequest, "invalid_request",
			"malformed or unexpected request body")
		return false
	}
	if err := dec.Decode(v); !errors.Is(err, io.EOF) {
		protocol.WriteError(w, http.StatusBadRequest, "invalid_request",
			"unexpected data after the JSON body")
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
