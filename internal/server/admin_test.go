/**
 * @file admin_test
 * @description Management API tests: the bearer guard, method guards,
 * the quota read/top-up pair and the breaker listing.
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
	admin := NewAdmin("secret", nil, nil, breakers)

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
	admin := NewAdmin("", nil, nil, nil) // empty token: open, as documented

	for _, method := range []string{http.MethodPost, http.MethodDelete} {
		req := httptest.NewRequest(method, "/admin/breakers", nil)
		rec := httptest.NewRecorder()
		admin.ServeHTTP(rec, req)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("%s status = %d, want 405 (the admin surface is read-only)", method, rec.Code)
		}
	}
}

func TestAdminQuotaTopUp(t *testing.T) {
	t.Parallel()
	received := int64(-1)
	setter := func(_ *http.Request, tenantID string, balance int64) error {
		if tenantID == "ghost" {
			return quota.ErrUnknownTenant
		}
		received = balance
		return nil
	}
	admin := NewAdmin("", nil, setter, nil)

	req := httptest.NewRequest(http.MethodPut, "/admin/tenants/t1/quota",
		strings.NewReader(`{"balance":500}`))
	rec := httptest.NewRecorder()
	admin.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || received != 500 {
		t.Fatalf("status = %d received = %d, want 200/500", rec.Code, received)
	}

	req = httptest.NewRequest(http.MethodPut, "/admin/tenants/ghost/quota",
		strings.NewReader(`{"balance":500}`))
	rec = httptest.NewRecorder()
	admin.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unknown tenant top-up = %d, want 404", rec.Code)
	}

	for _, body := range []string{`{}`, `{"balance":-1}`, `not json`} {
		req = httptest.NewRequest(http.MethodPut, "/admin/tenants/t1/quota",
			strings.NewReader(body))
		rec = httptest.NewRecorder()
		admin.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("body %q status = %d, want 400", body, rec.Code)
		}
	}

	// A typo'd extra member is rejected instead of silently ignored on
	// a balance write.
	req = httptest.NewRequest(http.MethodPut, "/admin/tenants/t1/quota",
		strings.NewReader(`{"balance":1,"persist":false}`))
	rec = httptest.NewRecorder()
	admin.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown member status = %d, want 400", rec.Code)
	}
	if received != 500 {
		t.Fatalf("a rejected top-up reached the ledger: %d", received)
	}
	if received == -1 {
		t.Fatal("a rejected top-up must not reach the ledger")
	}
}

func TestAdminQuotaRejectsWrongMethod(t *testing.T) {
	t.Parallel()
	admin := NewAdmin("", nil, nil, nil)

	// The quota path answers exactly GET (read) and PUT (top-up).
	req := httptest.NewRequest(http.MethodPost, "/admin/tenants/t1/quota", nil)
	rec := httptest.NewRecorder()
	admin.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST on the quota path = %d, want 405", rec.Code)
	}
	allow := rec.Header().Get("Allow")
	if !strings.Contains(allow, http.MethodGet) || !strings.Contains(allow, http.MethodPut) {
		t.Fatalf("Allow = %q, want GET and PUT advertised", allow)
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
	admin := NewAdmin("", balances, nil, nil)

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
	admin := NewAdmin("", nil, nil, states)

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

// TestAdminBreakersNilListRendersEmptyArray pins that an empty breaker
// table renders as [], never as JSON null.
func TestAdminBreakersNilListRendersEmptyArray(t *testing.T) {
	t.Parallel()
	admin := NewAdmin("", nil, nil, func(_ *http.Request) []BreakerView { return nil })

	req := httptest.NewRequest(http.MethodGet, "/admin/breakers", nil)
	rec := httptest.NewRecorder()
	admin.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"breakers":[]`) {
		t.Fatalf("status = %d body = %s, want an empty JSON array", rec.Code, rec.Body.String())
	}
}

// TestAdminUnknownPathsReturnNotFound pins the surface boundary: paths
// outside the three endpoints 404, including the quota path with an
// empty tenant id.
func TestAdminUnknownPathsReturnNotFound(t *testing.T) {
	t.Parallel()
	admin := NewAdmin("", nil, nil, nil)

	for _, path := range []string{"/admin/unknown", "/admin", "/admin/tenants//quota"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		admin.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Errorf("GET %s status = %d, want 404", path, rec.Code)
		}
	}
}

// TestAdminDisabledEndpointsReturnNotFound pins that a nil dependency
// removes its endpoint instead of failing cryptically at call time.
func TestAdminDisabledEndpointsReturnNotFound(t *testing.T) {
	t.Parallel()
	admin := NewAdmin("", nil, nil, nil)

	req := httptest.NewRequest(http.MethodGet, "/admin/tenants/t1/quota", nil)
	rec := httptest.NewRecorder()
	admin.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("quota read without a lookup = %d, want 404", rec.Code)
	}

	req = httptest.NewRequest(http.MethodPut, "/admin/tenants/t1/quota",
		strings.NewReader(`{"balance":5}`))
	rec = httptest.NewRecorder()
	admin.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("top-up without a writer = %d, want 404", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/admin/breakers", nil)
	rec = httptest.NewRecorder()
	admin.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("breakers without a state source = %d, want 404", rec.Code)
	}
}

// TestAdminTopUpLedgerFailureIsUnavailable pins that a write-path ledger
// outage renders as 503, never as a client-side status.
func TestAdminTopUpLedgerFailureIsUnavailable(t *testing.T) {
	t.Parallel()
	setter := func(_ *http.Request, _ string, _ int64) error { return errors.New("redis: connection refused") }
	admin := NewAdmin("", nil, setter, nil)

	req := httptest.NewRequest(http.MethodPut, "/admin/tenants/t1/quota",
		strings.NewReader(`{"balance":5}`))
	rec := httptest.NewRecorder()
	admin.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "balance_unavailable") {
		t.Fatalf("status = %d body = %s, want 503 envelope", rec.Code, rec.Body.String())
	}
}
