/**
 * @file admin_actions_test
 * @description The bound admin actions: the breaker reset lifts an
 * open machine for configured upstreams only, and the on-demand probe
 * distinguishes unknown, probe-less and failing upstreams.
 */
package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/daftpunkwav/breakwater/internal/circuit"
	"github.com/daftpunkwav/breakwater/internal/obs"
	"github.com/daftpunkwav/breakwater/internal/quota"
	"github.com/daftpunkwav/breakwater/internal/upstream"
)

// statusServer answers every request with the given status.
func statusServer(t *testing.T, status int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// probeAdapter builds an adapter whose probe endpoint is the server.
func probeAdapter(t *testing.T, id string, srv *httptest.Server) upstream.Upstream {
	t.Helper()
	adapter, err := upstream.NewOpenAI(upstream.OpenAIConfig{
		ID:       id,
		BaseURL:  srv.URL,
		ProbeURL: srv.URL + "/healthz",
	})
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	return adapter
}

// adminActions builds an admin handler with two configured upstreams:
// "u1" probe-capable, "noprobe" configured without a probe url. The
// probe target answers with the given status.
func adminActions(t *testing.T, probeStatus int) (http.Handler, circuit.Breaker) {
	t.Helper()
	srv := statusServer(t, probeStatus)
	breaker := circuit.NewRegistry(circuit.Config{FailThreshold: 1})
	gov := &governance{ledger: quota.NewMemory()}
	cfg := testConfig("127.0.0.1:0")
	cfg.Probe.Timeout = time.Second
	adapters := map[string]upstream.Upstream{
		"u1":      probeAdapter(t, "u1", srv),
		"noprobe": probeAdapter(t, "noprobe", srv),
	}
	probes := map[string]upstream.Upstream{"u1": adapters["u1"]}
	return buildAdmin(cfg, gov, breaker, obs.NewMetrics(), []string{"u1", "noprobe"}, adapters, probes), breaker
}

func TestAdminProbeEndpoint(t *testing.T) {
	t.Parallel()
	admin, _ := adminActions(t, http.StatusOK)

	post := func(id string) *httptest.ResponseRecorder {
		rec := httptest.NewRecorder()
		admin.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/admin/upstreams/"+id+"/probe", nil))
		return rec
	}

	if rec := post("u1"); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"ok":true`) {
		t.Fatalf("probe = %d %s, want 200 ok", rec.Code, rec.Body.String())
	}
	if rec := post("noprobe"); rec.Code != http.StatusConflict {
		t.Fatalf("probe without probe url = %d, want 409", rec.Code)
	}
	if rec := post("ghost"); rec.Code != http.StatusNotFound {
		t.Fatalf("probe unknown = %d, want 404", rec.Code)
	}
	// Wrong methods meet the guard, not the action.
	rec := httptest.NewRecorder()
	admin.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/admin/upstreams/u1/probe", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET probe = %d, want 405", rec.Code)
	}
}

func TestAdminProbeEndpointReportsFailures(t *testing.T) {
	t.Parallel()
	admin, _ := adminActions(t, http.StatusServiceUnavailable)

	rec := httptest.NewRecorder()
	admin.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/admin/upstreams/u1/probe", nil))
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("probe = %d, want 502 on a failing health endpoint", rec.Code)
	}
}

// TestAdminProbeWorksWithoutProbeTimeout: with the recovery loop
// disabled the operator may leave the probe timeout unset (the config
// only validates it when the loop is on) — the on-demand probe must
// still run instead of expiring before it starts.
func TestAdminProbeWorksWithoutProbeTimeout(t *testing.T) {
	t.Parallel()
	srv := statusServer(t, http.StatusOK)
	gov := &governance{ledger: quota.NewMemory()}
	cfg := testConfig("127.0.0.1:0")
	cfg.Probe.Interval = 0
	cfg.Probe.Timeout = 0
	adapters := map[string]upstream.Upstream{"u1": probeAdapter(t, "u1", srv)}
	admin := buildAdmin(cfg, gov, circuit.NopBreaker{}, obs.NewMetrics(), []string{"u1"}, adapters, adapters)

	rec := httptest.NewRecorder()
	admin.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/admin/upstreams/u1/probe", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"ok":true`) {
		t.Fatalf("probe = %d %s, want 200 ok without a configured timeout", rec.Code, rec.Body.String())
	}
}

func TestAdminBreakerResetEndpoint(t *testing.T) {
	t.Parallel()
	admin, breaker := adminActions(t, http.StatusOK)
	ctx := context.Background()

	// Force the breaker open the ordinary way.
	perm, ok := breaker.Allow(ctx, "u1")
	if !ok {
		t.Fatal("setup: closed breaker denied the call")
	}
	perm.Report(circuit.OutcomeServerFault)
	if breaker.StateOf(ctx, "u1") != circuit.StateOpen {
		t.Fatal("setup: breaker did not open")
	}

	rec := httptest.NewRecorder()
	admin.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/admin/breakers/u1/reset", nil))
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "closed") {
		t.Fatalf("reset = %d %s, want 200 closed", rec.Code, rec.Body.String())
	}
	if state := breaker.StateOf(ctx, "u1"); state != circuit.StateClosed {
		t.Fatalf("state = %v, want closed after the admin reset", state)
	}

	rec = httptest.NewRecorder()
	admin.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/admin/breakers/ghost/reset", nil))
	if rec.Code != http.StatusNotFound {
		t.Fatalf("reset unknown = %d, want 404", rec.Code)
	}
}
