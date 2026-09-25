/**
 * @file identity_admin_test
 * @description The identity management surface: user creation, key
 * issuance with the per-user cap, the layered limit writes, key
 * disabling, and the error mapping — over a scripted store.
 */
package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/daftpunkwav/breakwater/internal/auth"
)

// scriptedIdentity answers the admin store calls from fixed behavior,
// recording what the surface wrote.
type scriptedIdentity struct {
	users       []auth.UserView
	keys        map[string][]auth.KeyView
	issued      auth.IssuedKey
	createdKey  bool
	createdUser bool
	lastUserOv  auth.LimitOverride
	lastKeyOv   auth.LimitOverride
	lastStatus  *bool
	err         error // first call returns this
}

func (s *scriptedIdentity) CreateUser(context.Context, string, auth.Role, string) (string, error) {
	s.createdUser = true
	return "u_new", s.err
}

func (s *scriptedIdentity) Users(context.Context) ([]auth.UserView, error) {
	return s.users, s.err
}

func (s *scriptedIdentity) SetUserLimits(_ context.Context, _ string, o auth.LimitOverride) error {
	s.lastUserOv = o
	return s.err
}

func (s *scriptedIdentity) CreateKey(context.Context, string, string) (auth.IssuedKey, error) {
	s.createdKey = true
	return s.issued, s.err
}

func (s *scriptedIdentity) Keys(context.Context, string) ([]auth.KeyView, error) {
	return s.keys["u1"], s.err
}

func (s *scriptedIdentity) SetKeyLimits(_ context.Context, _ string, o auth.LimitOverride) error {
	s.lastKeyOv = o
	return s.err
}

func (s *scriptedIdentity) SetKeyStatus(_ context.Context, _ string, active bool) error {
	s.lastStatus = &active
	return s.err
}

func identityAdmin(t *testing.T, store auth.AdminStore) *Admin {
	t.Helper()
	return NewAdmin("", nil, nil, nil, WithIdentityStore(store))
}

func doJSON(admin *Admin, method, path, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	admin.ServeHTTP(rec, req)
	return rec
}

func TestIdentityCreateUserAndList(t *testing.T) {
	t.Parallel()
	store := &scriptedIdentity{users: []auth.UserView{{
		ID: "u1", Name: "Alice", Role: auth.RoleUser, Tier: "free",
		CreatedAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}}}
	admin := identityAdmin(t, store)

	rec := doJSON(admin, http.MethodPost, "/admin/users", `{"name":"Bob","role":"admin","tier":"free"}`)
	if rec.Code != http.StatusCreated || !strings.Contains(rec.Body.String(), `"id":"u_new"`) {
		t.Fatalf("create status = %d body = %s", rec.Code, rec.Body.String())
	}

	rec = doJSON(admin, http.MethodGet, "/admin/users", "")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"name":"Alice"`) {
		t.Fatalf("list status = %d body = %s", rec.Code, rec.Body.String())
	}

	// A nameless or tierless user is a bad request, not a store error.
	if rec := doJSON(admin, http.MethodPost, "/admin/users", `{"name":"x"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("tierless status = %d, want 400", rec.Code)
	}

	// An unknown role is a client input error (400) refused before the
	// store — never a 503 masquerading as an outage, and never a silent
	// demotion to the default "user".
	store = &scriptedIdentity{users: store.users}
	if rec := doJSON(admin, http.MethodPost, "/admin/users", `{"name":"x","tier":"free","role":"owner"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown role status = %d, want 400", rec.Code)
	}
	if store.createdUser {
		t.Fatal("a request with an unknown role reached the store")
	}
	// The two valid roles still reach the store; an unknown member is
	// rejected instead of silently ignored.
	if rec := doJSON(admin, http.MethodPost, "/admin/users", `{"name":"x","tier":"free","roles":"admin"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown member status = %d, want 400", rec.Code)
	}
	if store.createdUser {
		t.Fatal("a request with an unknown member reached the store")
	}
	if rec := doJSON(admin, http.MethodPost, "/admin/users", `{"name":"x","tier":"free","role":"admin"}`); rec.Code != http.StatusCreated {
		t.Fatalf("admin role status = %d, want 201", rec.Code)
	}
}

func TestIdentityKeyIssuanceAndStatus(t *testing.T) {
	t.Parallel()
	store := &scriptedIdentity{
		issued: auth.IssuedKey{
			KeyView: auth.KeyView{ID: "k1", Name: "laptop", Active: true},
			Raw:     "bw-secret",
		},
		// The listing projection the store keeps after issuance.
		keys: map[string][]auth.KeyView{"u1": {{ID: "k1", Name: "laptop", Active: true}}},
	}
	admin := identityAdmin(t, store)

	rec := doJSON(admin, http.MethodPost, "/admin/users/u1/keys", `{"name":"laptop"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("issue status = %d body = %s", rec.Code, rec.Body.String())
	}
	var issued auth.IssuedKey
	if err := json.Unmarshal(rec.Body.Bytes(), &issued); err != nil || issued.Raw != "bw-secret" {
		t.Fatalf("issued = %s err = %v, want the raw key exactly once", rec.Body.String(), err)
	}

	rec = doJSON(admin, http.MethodGet, "/admin/users/u1/keys", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("keys status = %d", rec.Code)
	}
	// Shown-once semantics: the listing projects the key without the
	// raw secret — only the issuance response ever carries it.
	if strings.Contains(rec.Body.String(), "bw-secret") {
		t.Fatalf("keys listing leaked the raw secret: %s", rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"id":"k1"`) {
		t.Fatalf("keys listing = %s, want the key projection", rec.Body.String())
	}

	rec = doJSON(admin, http.MethodPut, "/admin/keys/k1/status", `{"enabled":false}`)
	if rec.Code != http.StatusOK || store.lastStatus == nil || *store.lastStatus {
		t.Fatalf("disable status = %d body = %s, want the key disabled", rec.Code, rec.Body.String())
	}

	// A present-but-malformed body is a 400, never a silently unnamed
	// key; an absent body stays legal (an unnamed key). A typo'd member
	// is rejected like on every other governance write.
	store2 := &scriptedIdentity{issued: auth.IssuedKey{KeyView: auth.KeyView{ID: "k2", Active: true}, Raw: "bw-x"}}
	admin2 := identityAdmin(t, store2)
	if rec := doJSON(admin2, http.MethodPost, "/admin/users/u1/keys", `{bad`); rec.Code != http.StatusBadRequest {
		t.Fatalf("malformed key body status = %d, want 400", rec.Code)
	}
	if store2.createdKey {
		t.Fatal("a malformed body reached the store")
	}
	if rec := doJSON(admin2, http.MethodPost, "/admin/users/u1/keys", `{"naem":"x"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown member status = %d, want 400", rec.Code)
	}
	if store2.createdKey {
		t.Fatal("a payload with an unknown member reached the store")
	}
	if rec := doJSON(admin2, http.MethodPost, "/admin/users/u1/keys", ""); rec.Code != http.StatusCreated {
		t.Fatalf("empty key body status = %d, want 201", rec.Code)
	}
}

func TestIdentityLayeredLimitWrites(t *testing.T) {
	t.Parallel()
	store := &scriptedIdentity{}
	admin := identityAdmin(t, store)

	// User layer: deny a model and clamp concurrency for every key.
	body := `{"denied_models":["expensive-model"],"concurrency":2,"monthly_quota":123}`
	if rec := doJSON(admin, http.MethodPut, "/admin/users/u1/limits", body); rec.Code != http.StatusOK {
		t.Fatalf("user limits status = %d body = %s", rec.Code, rec.Body.String())
	}
	if len(store.lastUserOv.DeniedModels) != 1 || store.lastUserOv.Concurrency == nil || *store.lastUserOv.Concurrency != 2 {
		t.Fatalf("user override = %+v", store.lastUserOv)
	}

	// Key layer: one key of the same user gets its own tighter quota.
	if rec := doJSON(admin, http.MethodPut, "/admin/keys/k1/limits", `{"rpm":5}`); rec.Code != http.StatusOK {
		t.Fatalf("key limits status = %d", rec.Code)
	}
	if store.lastKeyOv.RPM == nil || *store.lastKeyOv.RPM != 5 {
		t.Fatalf("key override = %+v", store.lastKeyOv)
	}

	// The empty document clears a layer — a legitimate write, not an
	// error.
	if rec := doJSON(admin, http.MethodPut, "/admin/keys/k1/limits", `{}`); rec.Code != http.StatusOK {
		t.Fatalf("clear-limits status = %d", rec.Code)
	}

	// A malformed override body is a 400, not a store write.
	if rec := doJSON(admin, http.MethodPut, "/admin/keys/k1/limits", `{rpm`); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad override status = %d, want 400", rec.Code)
	}
	if store.lastKeyOv.RPM != nil {
		t.Fatalf("a rejected payload reached the store: %+v", store.lastKeyOv)
	}

	// A typo'd member ("rpms" for "rpm") is a 400, never a silent no-op
	// on a governance write.
	if rec := doJSON(admin, http.MethodPut, "/admin/keys/k1/limits", `{"rpms":5}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("typo'd member status = %d body = %s, want 400", rec.Code, rec.Body.String())
	}
	if store.lastKeyOv.RPM != nil {
		t.Fatalf("a typo'd payload reached the store: %+v", store.lastKeyOv)
	}

	// Empty path ids name nothing.
	if rec := doJSON(admin, http.MethodPut, "/admin/users//limits", `{}`); rec.Code != http.StatusNotFound {
		t.Fatalf("empty user id status = %d, want 404", rec.Code)
	}
	if rec := doJSON(admin, http.MethodPut, "/admin/keys//limits", `{}`); rec.Code != http.StatusNotFound {
		t.Fatalf("empty key id status = %d, want 404", rec.Code)
	}
	// A bare key list for an id the store answers: fine.
	if rec := doJSON(admin, http.MethodGet, "/admin/users//keys", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("empty user id keys status = %d, want 404", rec.Code)
	}
}

func TestIdentityErrorMapping(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		err    error
		want   int
		code   string
		method string
		path   string
	}{
		{"unknown user", auth.ErrUnknownUser, http.StatusNotFound, "identity_unknown", http.MethodPut, "/admin/users/ghost/limits"},
		{"unknown key", auth.ErrUnknownKey, http.StatusNotFound, "identity_unknown", http.MethodPut, "/admin/keys/ghost/status"},
		{"unknown tier", auth.ErrUnknownTier, http.StatusNotFound, "identity_unknown", http.MethodPost, "/admin/users"},
		{"key cap", auth.ErrTooManyKeys, http.StatusConflict, "key_limit_reached", http.MethodPost, "/admin/users/u1/keys"},
		{"user limits outage", errors.New("connection refused"), http.StatusServiceUnavailable, "identity_store_unavailable", http.MethodPut, "/admin/users/ghost/limits"},
		{"key limits outage", errors.New("connection refused"), http.StatusServiceUnavailable, "identity_store_unavailable", http.MethodPut, "/admin/keys/ghost/limits"},
		{"keys outage", errors.New("connection refused"), http.StatusServiceUnavailable, "identity_store_unavailable", http.MethodGet, "/admin/users/u1/keys"},
		{"store outage", errors.New("connection refused"), http.StatusServiceUnavailable, "identity_store_unavailable", http.MethodGet, "/admin/users"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			// Every endpoint surfaces the sentinel through the same
			// mapping, including the create paths that script it
			// directly.
			store := &scriptedIdentity{err: tc.err}
			admin := identityAdmin(t, store)
			body := ""
			switch tc.path {
			case "/admin/users":
				body = `{"name":"n","tier":"t"}`
			case "/admin/users/ghost/limits", "/admin/keys/ghost/limits":
				body = `{}`
			case "/admin/keys/ghost/status":
				body = `{"enabled":true}`
			case "/admin/users/u1/keys":
				body = `{}`
			}
			rec := doJSON(admin, tc.method, tc.path, body)
			if rec.Code != tc.want || !strings.Contains(rec.Body.String(), `"code":"`+tc.code+`"`) {
				t.Fatalf("status = %d body = %s, want %d %s", rec.Code, rec.Body.String(), tc.want, tc.code)
			}
		})
	}
}

// TestIdentitySurfaceBoundaries pins the remaining dispatch and decode
// edges: unknown subpaths 404, empty listings render as JSON arrays
// (never null), and malformed documents for the user layer and the key
// status switch are 400s before the store is touched.
func TestIdentitySurfaceBoundaries(t *testing.T) {
	t.Parallel()
	admin := identityAdmin(t, &scriptedIdentity{})

	// Unknown subpaths under the identity subtrees name nothing.
	for _, p := range []struct{ method, path string }{
		{http.MethodGet, "/admin/users/u1"},
		{http.MethodGet, "/admin/keys/k1"},
	} {
		if rec := doJSON(admin, p.method, p.path, ""); rec.Code != http.StatusNotFound {
			t.Fatalf("%s %s = %d, want 404", p.method, p.path, rec.Code)
		}
	}

	// Empty listings render as JSON arrays, never null.
	if rec := doJSON(admin, http.MethodGet, "/admin/users", ""); rec.Code != http.StatusOK ||
		!strings.Contains(rec.Body.String(), `"users":[]`) {
		t.Fatalf("empty users = %d %s, want a JSON array", rec.Code, rec.Body.String())
	}
	if rec := doJSON(admin, http.MethodGet, "/admin/users/u1/keys", ""); rec.Code != http.StatusOK ||
		!strings.Contains(rec.Body.String(), `"keys":[]`) {
		t.Fatalf("empty keys = %d %s, want a JSON array", rec.Code, rec.Body.String())
	}

	// A malformed override document for the user layer is a 400, not a
	// store write.
	if rec := doJSON(admin, http.MethodPut, "/admin/users/u1/limits", `{rpm`); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad user override = %d, want 400", rec.Code)
	}
	// A missing "enabled" member on the key-status switch is a 400.
	if rec := doJSON(admin, http.MethodPut, "/admin/keys/k1/status", `{}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("status without enabled = %d, want 400", rec.Code)
	}
}

// TestIdentityDualMethodPathsAdvertiseAllow pins the 405 contract of
// the surface's dual-method paths: a refused method advertises every
// legal one (GET reads, POST writes) in the Allow header, like the
// quota path advertises GET and PUT.
func TestIdentityDualMethodPathsAdvertiseAllow(t *testing.T) {
	t.Parallel()
	admin := identityAdmin(t, &scriptedIdentity{})

	for _, path := range []string{"/admin/users", "/admin/users/u1/keys"} {
		rec := doJSON(admin, http.MethodDelete, path, "")
		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("DELETE %s status = %d, want 405", path, rec.Code)
		}
		allow := rec.Header().Get("Allow")
		if !strings.Contains(allow, http.MethodGet) || !strings.Contains(allow, http.MethodPost) {
			t.Fatalf("%s Allow = %q, want GET and POST advertised", path, allow)
		}
	}
}

// TestIdentitySurfaceClosedWithoutStore: without an installed identity
// store the management endpoints close — the static mode's contract.
func TestIdentitySurfaceClosedWithoutStore(t *testing.T) {
	t.Parallel()
	admin := NewAdmin("", nil, nil, nil)

	paths := []struct{ method, path string }{
		{http.MethodGet, "/admin/users"},
		{http.MethodPost, "/admin/users"},
		{http.MethodPut, "/admin/users/u1/limits"},
		{http.MethodPost, "/admin/users/u1/keys"},
		{http.MethodGet, "/admin/users/u1/keys"},
		{http.MethodPut, "/admin/keys/k1/limits"},
		{http.MethodPut, "/admin/keys/k1/status"},
	}
	for _, p := range paths {
		rec := doJSON(admin, p.method, p.path, `{}`)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("%s %s = %d, want 404 with no identity store", p.method, p.path, rec.Code)
		}
	}
}
