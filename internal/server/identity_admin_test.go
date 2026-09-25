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
	users      []auth.UserView
	keys       map[string][]auth.KeyView
	issued     auth.IssuedKey
	createdKey bool
	lastUserOv auth.LimitOverride
	lastKeyOv  auth.LimitOverride
	lastStatus *bool
	err        error // first call returns this
}

func (s *scriptedIdentity) CreateUser(context.Context, string, auth.Role, string) (string, error) {
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
	var reader *strings.Reader
	if body == "" {
		reader = strings.NewReader("")
	} else {
		reader = strings.NewReader(body)
	}
	req := httptest.NewRequest(method, path, reader)
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
}

func TestIdentityKeyIssuanceAndStatus(t *testing.T) {
	t.Parallel()
	store := &scriptedIdentity{issued: auth.IssuedKey{
		KeyView: auth.KeyView{ID: "k1", Name: "laptop", Active: true},
		Raw:     "bw-secret",
	}}
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

	rec = doJSON(admin, http.MethodPut, "/admin/keys/k1/status", `{"enabled":false}`)
	if rec.Code != http.StatusOK || store.lastStatus == nil || *store.lastStatus {
		t.Fatalf("disable status = %d body = %s, want the key disabled", rec.Code, rec.Body.String())
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
		{"store outage", errors.New("connection refused"), http.StatusServiceUnavailable, "identity_store_unavailable", http.MethodGet, "/admin/users"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := &scriptedIdentity{err: tc.err}
			if tc.err == auth.ErrUnknownTier || tc.err == auth.ErrTooManyKeys {
				// The create paths script the sentinel directly.
				store = &scriptedIdentity{err: tc.err}
			}
			admin := identityAdmin(t, store)
			body := ""
			switch tc.path {
			case "/admin/users":
				body = `{"name":"n","tier":"t"}`
			case "/admin/users/ghost/limits":
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
