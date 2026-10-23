/**
 * @file adminbinding_test
 * @description The admin handler's assembly: the live ledger, the
 * upstream list and the breaker registry it is bound to, exercised
 * through the quota read/top-up and breaker-listing routes, and the
 * quota endpoints' absence while the surface cannot authenticate a
 * caller.
 */
package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/daftpunkwav/breakwater/internal/circuit"
	"github.com/daftpunkwav/breakwater/internal/config"
	"github.com/daftpunkwav/breakwater/internal/obs"
	"github.com/daftpunkwav/breakwater/internal/quota"
)

// adminFixture is the assembly the admin binding tests drive: a seeded
// in-memory ledger, a breaker registry and the upstream list.
type adminFixture struct {
	admin  http.Handler
	ledger *quota.Memory
}

// newAdminFixture assembles the admin surface for the given posture.
func newAdminFixture(t *testing.T, cfg config.Config) adminFixture {
	t.Helper()
	ledger := quota.NewMemory()
	if err := ledger.SetBalance(context.Background(), "t1", 250); err != nil {
		t.Fatalf("seed: %v", err)
	}
	gov := &governanceBackends{ledger: ledger}
	return adminFixture{
		admin: buildAdmin(cfg, adminBindings{
			gov:         gov,
			breaker:     circuit.NopBreaker{},
			metrics:     obs.NewMetrics(),
			upstreamIDs: []string{"u1"},
		}),
		ledger: ledger,
	}
}

// TestBuildAdminEndpoints: the guarded admin surface serves the quota
// read/top-up and the breaker listing against the live backends.
func TestBuildAdminEndpoints(t *testing.T) {
	t.Parallel()
	cfg := testConfig("127.0.0.1:0")
	cfg.Security.AdminToken = "test-admin"
	fx := newAdminFixture(t, cfg)

	rec := httptest.NewRecorder()
	fx.admin.ServeHTTP(rec, adminRequest(http.MethodGet, "/admin/tenants/t1/quota", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"balance":250`) {
		t.Fatalf("GET = %d %s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	fx.admin.ServeHTTP(rec, adminRequest(http.MethodPut, "/admin/tenants/t1/quota",
		strings.NewReader(`{"balance":1000}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT = %d", rec.Code)
	}
	if bal, _ := fx.ledger.Balance(context.Background(), "t1"); bal != 1000 {
		t.Fatalf("top-up did not land: %d", bal)
	}

	rec = httptest.NewRecorder()
	fx.admin.ServeHTTP(rec, adminRequest(http.MethodGet, "/admin/breakers", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"upstream":"u1"`) {
		t.Fatalf("breakers = %d %s", rec.Code, rec.Body.String())
	}
}

// TestBuildAdminOmitsQuotaEndpointsWithoutAuthentication: a balance
// write rewrites money state, and a surface without a bearer token
// admits every caller, so the quota endpoints stay unmounted until the
// deployment can authenticate one.
func TestBuildAdminOmitsQuotaEndpointsWithoutAuthentication(t *testing.T) {
	t.Parallel()
	cfg := testConfig("127.0.0.1:0")
	cfg.Security.AdminToken = ""
	cfg.Security.AllowUnauthenticated = false
	fx := newAdminFixture(t, cfg)

	rec := httptest.NewRecorder()
	fx.admin.ServeHTTP(rec, adminRequest(http.MethodPut, "/admin/tenants/t1/quota",
		strings.NewReader(`{"balance":1000}`)))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("PUT = %d, want 404 (the endpoint is not mounted)", rec.Code)
	}
	if bal, _ := fx.ledger.Balance(context.Background(), "t1"); bal != 250 {
		t.Fatalf("an unauthenticated write landed: %d", bal)
	}

	rec = httptest.NewRecorder()
	fx.admin.ServeHTTP(rec, adminRequest(http.MethodGet, "/admin/tenants/t1/quota", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("GET = %d, want 404 (the endpoint is not mounted)", rec.Code)
	}
}

// TestBuildAdminKeepsQuotaEndpointsOnTheUnauthenticatedOptIn: the
// operator's explicit opt-in accepts an open admin surface, so the
// quota endpoints stay mounted without a bearer token.
func TestBuildAdminKeepsQuotaEndpointsOnTheUnauthenticatedOptIn(t *testing.T) {
	t.Parallel()
	cfg := testConfig("127.0.0.1:0")
	cfg.Security.AdminToken = ""
	cfg.Security.AllowUnauthenticated = true
	fx := newAdminFixture(t, cfg)

	rec := httptest.NewRecorder()
	fx.admin.ServeHTTP(rec, adminRequest(http.MethodPut, "/admin/tenants/t1/quota",
		strings.NewReader(`{"balance":1000}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT = %d, want the endpoint mounted on the explicit opt-in", rec.Code)
	}
	if bal, _ := fx.ledger.Balance(context.Background(), "t1"); bal != 1000 {
		t.Fatalf("the opted-in write did not land: %d", bal)
	}
}

// adminRequest builds an admin request carrying the fixture's bearer
// token.
func adminRequest(method, path string, body io.Reader) *http.Request {
	req := httptest.NewRequest(method, path, body)
	req.Header.Set("Authorization", "Bearer test-admin")
	return req
}
