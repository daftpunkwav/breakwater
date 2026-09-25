/**
 * @file serve_test
 * @description Lifecycle tests of the assembled gateway: serve-until-
 * cancelled with and without governance, startup failures surfacing as
 * returned errors, and the helpers of the assembly.
 */
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/daftpunkwav/breakwater/internal/auth"
	"github.com/daftpunkwav/breakwater/internal/circuit"
	"github.com/daftpunkwav/breakwater/internal/config"
	"github.com/daftpunkwav/breakwater/internal/obs"
	"github.com/daftpunkwav/breakwater/internal/quota"
)

// baseEnv returns the environment entries of a minimal working gateway:
// one mock upstream, one static identity, an ephemeral port.
func baseEnv(addr string) []string {
	return []string{
		"BREAKWATER_ADDR=" + addr,
		`BREAKWATER_UPSTREAMS=[{"id":"mock","base_url":"http://127.0.0.1:1","models":["*"]}]`,
		`BREAKWATER_IDENTITY={"tiers":[{"id":"free","rpm":10,"tpm":1000,"max_tokens":64,"monthly_quota":1000,"allowed_models":["*"]}],"tenants":[{"id":"t1","name":"T1","tier":"free","keys":["k1"]}]}`,
	}
}

func setEnv(t *testing.T, entries []string) {
	t.Helper()
	for _, key := range []string{
		"BREAKWATER_REDIS_ADDR", "BREAKWATER_POSTGRES_DSN", "BREAKWATER_ACCESS_LOG_PATH",
		"BREAKWATER_UPSTREAMS", "BREAKWATER_IDENTITY", "BREAKWATER_ADDR",
	} {
		t.Setenv(key, "")
	}
	for _, entry := range entries {
		key, value, _ := strings.Cut(entry, "=")
		t.Setenv(key, value)
	}
}

// TestServeRunsAndStops pins the full assembled lifecycle: a cancelled
// parent context ends serve cleanly with a nil error.
func TestServeRunsAndStops(t *testing.T) {
	setEnv(t, baseEnv("127.0.0.1:0"))
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(150 * time.Millisecond) // let the listener come up
		cancel()
	}()
	if err := serve(ctx, cfg, slog.New(slog.DiscardHandler), "test"); err != nil {
		t.Fatalf("serve: %v", err)
	}
}

// TestServeRedisModeWithIdentityAndReconciliation drives the full
// assembly: Redis backends, static identity, balance seeding, the
// reconcile worker and the fail-closed pipeline — everything the
// memory-mode test skips.
func TestServeRedisModeWithIdentityAndReconciliation(t *testing.T) {
	mr := miniredis.RunT(t)
	setEnv(t, baseEnv("127.0.0.1:0"))
	t.Setenv("BREAKWATER_REDIS_ADDR", mr.Addr())
	t.Setenv("BREAKWATER_POSTGRES_DSN", "postgres://breakwater:breakwater@127.0.0.1:1/db")
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(150 * time.Millisecond)
		cancel()
	}()
	if err := serve(ctx, cfg, slog.New(slog.DiscardHandler), "test"); err != nil {
		t.Fatalf("serve: %v", err)
	}
}

// TestServeWithoutIdentity covers the warn branch: a gateway with
// upstreams but no identity serves business routes without governance.
func TestServeWithoutIdentity(t *testing.T) {
	setEnv(t, []string{
		"BREAKWATER_ADDR=127.0.0.1:0",
		`BREAKWATER_UPSTREAMS=[{"id":"mock","base_url":"http://127.0.0.1:1","models":["*"]}]`,
		"BREAKWATER_IDENTITY=",
	})
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()
	if err := serve(ctx, cfg, slog.New(slog.DiscardHandler), "test"); err != nil {
		t.Fatalf("serve: %v", err)
	}
}

// TestServeReturnsStartupErrors pins that assembly failures surface as
// returned errors instead of exit codes. Each branch below trips a
// different assembly step after config.Load has passed.
func TestServeReturnsStartupErrors(t *testing.T) {
	t.Run("identity store", func(t *testing.T) {
		setEnv(t, baseEnv("127.0.0.1:0"))
		t.Setenv("BREAKWATER_POSTGRES_DSN", "not a valid dsn")
		if err := run(context.Background(), slog.New(slog.DiscardHandler), "test"); err == nil {
			t.Fatal("assembly failure must surface as a returned error")
		}
	})

	t.Run("access log path", func(t *testing.T) {
		setEnv(t, baseEnv("127.0.0.1:0")) // DSN stays empty: static identity
		t.Setenv("BREAKWATER_ACCESS_LOG_PATH", filepath.Join(t.TempDir(), "missing-dir", "a.log"))
		if err := run(context.Background(), slog.New(slog.DiscardHandler), "test"); err == nil {
			t.Fatal("an unopenable access log must fail assembly")
		}
	})

	t.Run("dead redis", func(t *testing.T) {
		setEnv(t, baseEnv("127.0.0.1:0"))
		t.Setenv("BREAKWATER_REDIS_ADDR", "127.0.0.1:1")
		if err := run(context.Background(), slog.New(slog.DiscardHandler), "test"); err == nil {
			t.Fatal("dead redis: assembly failure must surface as a returned error")
		}
	})

	t.Run("busy port", func(t *testing.T) {
		// Occupy the port first: exercises the serve error path
		// (observation flush, then the returned error). The exact same
		// address form is reused — a bare ":port" could still bind on
		// the dual-stack wildcard.
		blocker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
		defer blocker.Close()
		setEnv(t, baseEnv(blocker.Listener.Addr().String()))
		if err := run(context.Background(), slog.New(slog.DiscardHandler), "test"); err == nil {
			t.Fatal("a busy port must surface as a returned error")
		}
	})
}

// TestRunRejectsBadConfiguration covers the run wrapper: an invalid
// environment is rejected before anything is assembled.
func TestRunRejectsBadConfiguration(t *testing.T) {
	setEnv(t, baseEnv("127.0.0.1:0"))
	t.Setenv("BREAKWATER_STREAM_TIMEOUT", "-5s")
	if err := run(context.Background(), slog.New(slog.DiscardHandler), "test"); err == nil {
		t.Fatal("run must reject an invalid configuration")
	}
}

func TestMergeReadiness(t *testing.T) {
	t.Parallel()
	if mergeReadiness(nil, nil) != nil {
		t.Fatal("all-nil probes must report ready (nil)")
	}

	okProbe := func() error { return nil }
	badProbe := func() error { return errors.New("redis down") }

	if err := mergeReadiness(nil, okProbe)(); err != nil {
		t.Fatalf("nil probes must drop out: %v", err)
	}
	if err := mergeReadiness(okProbe, badProbe)(); err == nil {
		t.Fatal("one failing probe must fail the merge")
	}
}

func TestStateValue(t *testing.T) {
	t.Parallel()
	if stateValue(circuit.StateClosed) != 0 ||
		stateValue(circuit.StateHalfOpen) != 1 ||
		stateValue(circuit.StateOpen) != 2 {
		t.Fatal("state gauge mapping drifted")
	}
}

func TestMetricsHandlerRenders(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	metricsHandler(obs.NewMetrics()).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if ct := rec.Header().Get("Content-Type"); !strings.Contains(ct, "text/plain") {
		t.Fatalf("content type = %q", ct)
	}
	if rec.Body.Len() == 0 {
		t.Fatal("exposition must not be empty")
	}
}

func TestBuildBindingsValidates(t *testing.T) {
	t.Parallel()
	if _, err := buildBindings([]config.Upstream{{ID: "broken"}}); err == nil {
		t.Fatal("upstream without base_url must fail binding")
	}
	bindings, err := buildBindings([]config.Upstream{{ID: "ok", BaseURL: "http://x", Models: []string{"*"}}})
	if err != nil || len(bindings) != 1 {
		t.Fatalf("bindings = %v err = %v", bindings, err)
	}
}

func TestNewAccessLogDisabledAndEnabled(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.DiscardHandler)

	disabled, err := newAccessLog(config.Config{}, logger)
	if err != nil || disabled.logger != nil || disabled.sink != nil {
		t.Fatalf("no path configured must disable the access log: %v", err)
	}
	disabled.close() // must be safe when disabled

	path := filepath.Join(t.TempDir(), "access.log")
	enabled, err := newAccessLog(config.Config{Obs: config.Obs{AccessLogPath: path}}, logger)
	if err != nil {
		t.Fatalf("open access log: %v", err)
	}
	if enabled.logger == nil {
		t.Fatal("configured path must enable the access log")
	}
	// An unopenable path is an error, never a silently unobserved gateway.
	if _, err := newAccessLog(config.Config{Obs: config.Obs{AccessLogPath: filepath.Join(t.TempDir(), "missing-dir", "a.log")}}, logger); err == nil {
		t.Fatal("unopenable path must fail assembly")
	}
	enabled.sink.Record(obs.Entry{Status: 200})
	// close drains the logger AND releases the file (Windows locks open
	// files, so TempDir cleanup would fail otherwise).
	enabled.close()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("access log file missing: %v", err)
	}
}

func TestSeedBalancesProvisionsTenants(t *testing.T) {
	t.Parallel()
	identity, err := auth.NewStatic(auth.StaticConfig{
		Tiers:   []auth.StaticTier{{ID: "free", RPM: 10, TPM: 1000, MaxTokens: 64, MonthlyQuota: 1000}},
		Tenants: []auth.StaticTenant{{ID: "t1", Name: "T1", Tier: "free", Keys: []string{"k1"}}},
	})
	if err != nil {
		t.Fatalf("identity: %v", err)
	}
	ledger := quota.NewMemory()
	seedBalances(context.Background(), identity, ledger, slog.New(slog.DiscardHandler))

	bal, err := ledger.Balance(context.Background(), "t1")
	if err != nil || bal != 1000 {
		t.Fatalf("balance = %d err = %v, want the tier monthly quota", bal, err)
	}
}
