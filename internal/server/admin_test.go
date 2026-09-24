/**
 * @file admin_test
 * @description Management API tests: the bearer guard, the GET method
 * guard and the two read-only endpoints.
 */
package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/daftpunkwav/breakwater/internal/circuit"
	"github.com/daftpunkwav/breakwater/internal/quota"
)

func TestAdminRequiresBearerToken(t *testing.T) {
	t.Parallel()
	breakers := func(_ *http.Request) []BreakerView { return []BreakerView{} }
	admin := NewAdmin("secret", nil, breakers)

	req := httptest.NewRequest(http.MethodGet, "/admin/breakers", nil)
	rec := httptest.NewRecorder()
	admin.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 without a token", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/admin/breakers", nil)
	req.Header.Set("Authorization", "Bearer wrong")
	rec = httptest.NewRecorder()
	admin.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 with a wrong token", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/admin/breakers", nil)
	req.Header.Set("Authorization", "Bearer secret")
	rec = httptest.NewRecorder()
	admin.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 with the right token", rec.Code)
	}
}

func TestAdminRejectsNonGetMethods(t *testing.T) {
	t.Parallel()
	admin := NewAdmin("", nil, nil) // empty token: open, as documented

	for _, method := range []string{http.MethodPost, http.MethodDelete} {
		req := httptest.NewRequest(method, "/admin/breakers", nil)
		rec := httptest.NewRecorder()
		admin.ServeHTTP(rec, req)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%s status = %d, want 405 (the admin surface is read-only)", method, rec.Code)
		}
	}
}

func TestAdminQuotaEndpoint(t *testing.T) {
	t.Parallel()
	balances := func(_ *http.Request, tenantID string) (int64, error) {
		switch tenantID {
		case "ghost":
			return 0, quota.ErrUnknownTenant
		case "broken":
			return 0, errors.New("redis: connection refused")
		}
		return 42, nil
	}
	admin := NewAdmin("", balances, nil)

	req := httptest.NewRequest(http.MethodGet, "/admin/tenants/t1/quota", nil)
	rec := httptest.NewRecorder()
	admin.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"balance":42`) {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}

	req = httptest.NewRequest(http.MethodGet, "/admin/tenants/ghost/quota", nil)
	rec = httptest.NewRecorder()
	admin.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown tenant status = %d, want 404", rec.Code)
	}

	// A ledger outage is a 503, never a misleading 404.
	req = httptest.NewRequest(http.MethodGet, "/admin/tenants/broken/quota", nil)
	rec = httptest.NewRecorder()
	admin.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "balance_unavailable") {
		t.Fatalf("outage status = %d body = %s, want 503 envelope", rec.Code, rec.Body.String())
	}
}

func TestAdminBreakersEndpoint(t *testing.T) {
	t.Parallel()
	states := func(_ *http.Request) []BreakerView {
		return []BreakerView{{Upstream: "u1", State: circuit.StateClosed}}
	}
	admin := NewAdmin("", nil, states)

	req := httptest.NewRequest(http.MethodGet, "/admin/breakers", nil)
	rec := httptest.NewRecorder()
	admin.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var body struct {
		Breakers []BreakerView `json:"breakers"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || len(body.Breakers) != 1 || body.Breakers[0].Upstream != "u1" {
		t.Fatalf("body = %s err = %v", rec.Body.String(), err)
	}
}
