/**
 * @file admin_actions_test
 * @description The breaker-reset and on-demand probe endpoints: route
 * and method guards, the sentinel error mapping, and the wire forms.
 */
package server

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/daftpunkwav/breakwater/internal/router"
)

func TestAdminBreakerResetEndpoint(t *testing.T) {
	t.Parallel()
	var resetFor string
	admin := NewAdmin("", nil, nil, nil, WithBreakerReset(func(_ *http.Request, id string) error {
		resetFor = id
		return nil
	}))

	post := func(path string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		admin.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, nil))
		return rec
	}

	rec := post("/admin/breakers/u1/reset")
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"state":"closed"`) {
		t.Fatalf("reset = %d %s, want 200 closed", rec.Code, rec.Body.String())
	}
	if resetFor != "u1" {
		t.Fatalf("reset invoked for %q, want u1", resetFor)
	}

	// An unknown upstream is a 404, any other failure a 500 — under
	// either sentinel vocabulary: the admin bindings report this
	// package's sentinel, the routing switch reports router's, and the
	// mapping must not confuse the second with a backend failure.
	admin2 := NewAdmin("", nil, nil, nil, WithBreakerReset(func(*http.Request, string) error {
		return ErrUnknownUpstream
	}))
	rec = httptest.NewRecorder()
	admin2.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/admin/breakers/u1/reset", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("reset unknown = %d, want 404", rec.Code)
	}

	adminRouterSentinel := NewAdmin("", nil, nil, nil, WithBreakerReset(func(*http.Request, string) error {
		return router.ErrUnknownUpstream
	}))
	rec = httptest.NewRecorder()
	adminRouterSentinel.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/admin/breakers/u1/reset", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("reset router-unknown = %d, want 404", rec.Code)
	}

	admin3 := NewAdmin("", nil, nil, nil, WithBreakerReset(func(*http.Request, string) error {
		return errors.New("registry gone")
	}))
	rec = httptest.NewRecorder()
	admin3.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/admin/breakers/u1/reset", nil))
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("reset failure = %d, want 500", rec.Code)
	}

	// Wrong methods meet the guard.
	admin4 := NewAdmin("", nil, nil, nil, WithBreakerReset(func(*http.Request, string) error { return nil }))
	rec = httptest.NewRecorder()
	admin4.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/admin/breakers/u1/reset", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET reset = %d, want 405", rec.Code)
	}
}

func TestAdminUpstreamProbeEndpoint(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"healthy", nil, http.StatusOK},
		{"unknown upstream", ErrUnknownUpstream, http.StatusNotFound},
		{"router-unknown upstream", router.ErrUnknownUpstream, http.StatusNotFound},
		{"no probe url", ErrProbeUnconfigured, http.StatusConflict},
		{"probe failed", errors.New("probe status 503"), http.StatusBadGateway},
	}
	for _, tc := range cases {
		admin := NewAdmin("", nil, nil, nil, WithUpstreamProbe(func(_ *http.Request, _ string) error {
			return tc.err
		}))
		rec := httptest.NewRecorder()
		admin.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/admin/upstreams/u1/probe", nil))
		if rec.Code != tc.want {
			t.Errorf("%s: probe = %d, want %d", tc.name, rec.Code, tc.want)
		}
		if tc.want == http.StatusOK && !strings.Contains(rec.Body.String(), `"ok":true`) {
			t.Errorf("%s: body = %s, want ok", tc.name, rec.Body.String())
		}
	}

	// Without the action installed the route reads as absent.
	admin := NewAdmin("", nil, nil, nil)
	rec := httptest.NewRecorder()
	admin.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/admin/upstreams/u1/probe", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("probe without action = %d, want 404", rec.Code)
	}
}
