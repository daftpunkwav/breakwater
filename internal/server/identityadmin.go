/**
 * @file identityadmin
 * @description The identity management endpoints of the admin surface:
 * user lifecycle, API-key issuance and status, and the layered limit
 * overrides — the administration half of the management API.
 *
 * Responsibilities:
 * - Route and serve the /admin/users and /admin/keys subtrees: create
 *   and list users, issue and list keys, replace either override
 *   layer, enable or disable a key
 * - Map the AdminStore's sentinel errors onto the surface's status
 *   codes: 404 identity_unknown, 409 key_limit_reached, 503
 *   identity_store_unavailable
 * - Nothing else: the bearer guard is the admin handler's; what an
 *   override means is the auth package's merge semantics; enforcing
 *   the merged result belongs to the governance stages
 *
 * Split from admin.go by subject: that file owns the surface's guard,
 * dispatch and the operations endpoints (quota, breakers, routing
 * switches); this file owns everything that writes identities.
 */
package server

import (
	"errors"
	"net/http"
	"strings"

	"github.com/daftpunkwav/breakwater/internal/auth"
	"github.com/daftpunkwav/breakwater/internal/protocol"
)

// serveIdentity routes the identity management subtree. Every path
// that enters answers exactly once — the dispatch itself 404s what no
// endpoint claims, and the dual-method paths (GET reads, POST writes)
// advertise both in the 405's Allow header, like the quota path does.
func (a *Admin) serveIdentity(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.URL.Path == "/admin/users":
		switch r.Method {
		case http.MethodPost:
			a.guarded(w, r, http.MethodPost, a.serveCreateUser)
		case http.MethodGet:
			a.guarded(w, r, http.MethodGet, a.serveListUsers)
		default:
			w.Header().Set("Allow", http.MethodGet+", "+http.MethodPost)
			w.WriteHeader(http.StatusMethodNotAllowed)
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
			switch r.Method {
			case http.MethodPost:
				a.guarded(w, r, http.MethodPost, func(w http.ResponseWriter, r *http.Request) {
					a.serveCreateKey(w, r, id)
				})
			case http.MethodGet:
				a.guarded(w, r, http.MethodGet, func(w http.ResponseWriter, r *http.Request) {
					a.serveListKeys(w, r, id)
				})
			default:
				w.Header().Set("Allow", http.MethodGet+", "+http.MethodPost)
				w.WriteHeader(http.StatusMethodNotAllowed)
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
	default:
		http.NotFound(w, r)
	}
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
	if !decodeAdminJSON(w, r, &body) {
		return
	}
	if body.Name == "" || body.Tier == "" {
		protocol.WriteError(w, http.StatusBadRequest, "invalid_request",
			`body must be {"name": string, "tier": string, "role": "user"|"admin" (default user)}`)
		return
	}
	// The role is validated here, before the store: a typo is a client
	// input error (400), not an identity-store outage (503), and an
	// unrecognized role must never silently become the default "user".
	role := auth.RoleUser
	switch body.Role {
	case "":
	case string(auth.RoleUser), string(auth.RoleAdmin):
		role = auth.Role(body.Role)
	default:
		protocol.WriteError(w, http.StatusBadRequest, "invalid_request",
			`role must be "user" or "admin"`)
		return
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
// returned exactly once — the store keeps only the hash. The body is
// optional (an unnamed key); a present-but-malformed body is a 400,
// never a silently unnamed key, and a typo'd member is rejected like
// on every other governance write.
func (a *Admin) serveCreateKey(w http.ResponseWriter, r *http.Request, userID string) {
	if a.identity == nil || userID == "" {
		http.NotFound(w, r)
		return
	}
	var body struct {
		Name string `json:"name"`
	}
	if !decodeAdminJSONOptional(w, r, &body) {
		return
	}
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
	if !decodeAdminJSON(w, r, &o) {
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
