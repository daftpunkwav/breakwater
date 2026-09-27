/**
 * @file adminbinding_test
 * @description The admin handler's assembly: the live ledger, the
 * upstream list and the breaker registry it is bound to, exercised
 * through the quota read/top-up and breaker-listing routes.
 */
package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/daftpunkwav/breakwater/internal/circuit"
	"github.com/daftpunkwav/breakwater/internal/obs"
	"github.com/daftpunkwav/breakwater/internal/quota"
)

func TestBuildAdminEndpoints(t *testing.T) {
	t.Parallel()
	ledger := quota.NewMemory()
	if err := ledger.SetBalance(context.Background(), "t1", 250); err != nil {
		t.Fatalf("seed: %v", err)
	}
	gov := &governance{ledger: ledger}
	breaker := circuit.NopBreaker{}
	admin := buildAdmin(testConfig("127.0.0.1:0"), gov, breaker, obs.NewMetrics(), []string{"u1"}, nil, nil)

	rec := httptest.NewRecorder()
	admin.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/admin/tenants/t1/quota", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"balance":250`) {
		t.Fatalf("GET = %d %s", rec.Code, rec.Body.String())
	}

	rec = httptest.NewRecorder()
	admin.ServeHTTP(rec, httptest.NewRequest(http.MethodPut, "/admin/tenants/t1/quota",
		strings.NewReader(`{"balance":1000}`)))
	if rec.Code != http.StatusOK {
		t.Fatalf("PUT = %d", rec.Code)
	}
	if bal, _ := ledger.Balance(context.Background(), "t1"); bal != 1000 {
		t.Fatalf("top-up did not land: %d", bal)
	}

	rec = httptest.NewRecorder()
	admin.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/admin/breakers", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"upstream":"u1"`) {
		t.Fatalf("breakers = %d %s", rec.Code, rec.Body.String())
	}
}
