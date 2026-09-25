/**
 * @file assembly_test
 * @description Assembly helper tests: the Redis governance branch
 * (miniredis), the breaker builder's two modes, the admin binding end
 * to end, the reconcile worker's startup and the log-drop publisher.
 */
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
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
	"github.com/redis/go-redis/v9"
)

func testConfig(addr string) config.Config {
	cfg, err := config.Load()
	if err != nil {
		panic(err)
	}
	cfg.Server.Addr = addr
	return cfg
}

func TestNewGovernanceMemoryMode(t *testing.T) {
	t.Parallel()
	gov, err := newGovernance(context.Background(), testConfig("127.0.0.1:0"))
	if err != nil {
		t.Fatalf("governance: %v", err)
	}
	defer gov.close()
	if gov.mode != "memory" || gov.limiter == nil || gov.ledger == nil || gov.sweepTarget == nil {
		t.Fatalf("memory governance incomplete: %+v", gov)
	}
	if gov.readiness != nil {
		t.Fatal("memory mode has no dependency to gate readiness on")
	}
}

func TestNewGovernanceRejectsDeadBackend(t *testing.T) {
	t.Parallel()
	cfg := testConfig("127.0.0.1:0")
	cfg.Redis.Addr = "127.0.0.1:1" // nothing listens here
	if _, err := newGovernance(context.Background(), cfg); err == nil {
		t.Fatal("a dead Redis at startup must fail assembly: the fail-closed limiter cannot serve")
	}
}

func TestNewAuthStoreBranches(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.DiscardHandler)

	// No identity: no store, no probe, nothing to close.
	store, static, closeFn, probe, err := newAuthStore(context.Background(), testConfig("127.0.0.1:0"))
	if err != nil || store != nil || static != nil || probe != nil {
		t.Fatalf("no identity = %+v err = %v, want all nil", store, err)
	}
	closeFn()

	// Broken identity JSON fails at assembly.
	bad := testConfig("127.0.0.1:0")
	bad.Identity = "{not json"
	if _, _, _, _, err := newAuthStore(context.Background(), bad); err == nil {
		t.Fatal("broken identity JSON must fail assembly")
	}

	// A syntactically valid DSN assembles the pg store lazily: the
	// readiness probe surfaces the (here unreachable) database.
	pg := testConfig("127.0.0.1:0")
	pg.Postgres.DSN = "postgres://breakwater:breakwater@127.0.0.1:1/db"
	pgStore, _, pgClose, pgProbe, err := newAuthStore(context.Background(), pg)
	if err != nil || pgStore == nil || pgProbe == nil {
		t.Fatalf("pg branch: %v", err)
	}
	defer pgClose()
	if err := pgProbe(); err == nil {
		t.Fatal("probe against an unreachable database must fail")
	}
	_ = logger
}

func TestSeedBalancesSkipsAndReports(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.DiscardHandler)

	// A failing seeder surfaces through the log, not a panic.
	if err := quota.NewMemory().SetBalance(context.Background(), "t", 1); err != nil {
		t.Fatalf("sanity: %v", err)
	}
	identity, err := auth.NewStatic(auth.StaticConfig{
		Tiers:   []auth.StaticTier{{ID: "free", MonthlyQuota: 0}}, // zero quota: skipped
		Tenants: []auth.StaticTenant{{ID: "t0", Name: "T0", Tier: "free", Keys: []string{"k"}}},
	})
	if err != nil {
		t.Fatalf("identity: %v", err)
	}
	seedBalances(context.Background(), identity, quota.NewMemory(), logger) // must not panic
}

func TestBuildBreakerHalfOpenObserver(t *testing.T) {
	t.Parallel()
	metrics := obs.NewMetrics()
	breaker := buildBreaker(config.Circuit{Enabled: true, FailThreshold: 1, Cooldown: 2 * time.Millisecond, ProbeTimeout: time.Second}, metrics)

	perm, ok := breaker.Allow(context.Background(), "u")
	if !ok {
		t.Fatal("closed breaker denied")
	}
	perm.Report(circuit.OutcomeServerFault) // -> open

	time.Sleep(5 * time.Millisecond) // cooldown elapses
	if _, ok := breaker.Allow(context.Background(), "u"); !ok {
		t.Fatal("half-open must admit the probe")
	}
}

func TestNewAccessLogEnabled(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "access.log")
	logger := slog.New(slog.DiscardHandler)
	bundle, err := newAccessLog(config.Config{Obs: config.Obs{AccessLogPath: path, AccessLogQueueSize: 8}}, logger)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	bundle.close()
}

func TestNewGovernanceRedisMode(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	cfg := testConfig("127.0.0.1:0")
	cfg.Redis.Addr = mr.Addr()

	gov, err := newGovernance(context.Background(), cfg)
	if err != nil {
		t.Fatalf("governance: %v", err)
	}
	defer gov.close()

	if gov.mode != "redis" || gov.redisClient == nil || gov.snapshotSource == nil {
		t.Fatalf("redis governance incomplete: mode=%s", gov.mode)
	}
	if err := gov.readiness(); err != nil {
		t.Fatalf("readiness against a live miniredis: %v", err)
	}

	// A dead backend must fail the readiness probe: the fail-closed
	// limiter would reject everything, so the probe may not lie.
	mr.Close()
	if err := gov.readiness(); err == nil {
		t.Fatal("readiness must gate on the Redis backend")
	}
}

func TestBuildBreakerBothModes(t *testing.T) {
	t.Parallel()
	metrics := obs.NewMetrics()

	if _, ok := buildBreaker(config.Circuit{Enabled: false}, metrics).(circuit.NopBreaker); !ok {
		t.Fatal("disabled circuit must compose the nop breaker")
	}

	breaker := buildBreaker(config.Circuit{Enabled: true, FailThreshold: 1, Cooldown: time.Hour, ProbeTimeout: time.Second}, metrics)
	perm, ok := breaker.Allow(context.Background(), "u")
	if !ok {
		t.Fatal("enabled breaker denied a fresh upstream")
	}
	perm.Report(circuit.OutcomeServerFault) // closed -> open, observed
	if got := breaker.StateOf(context.Background(), "u"); got != circuit.StateOpen {
		t.Fatalf("state = %s, want open", got)
	}
}

func TestBuildAdminEndpoints(t *testing.T) {
	t.Parallel()
	ledger := quota.NewMemory()
	if err := ledger.SetBalance(context.Background(), "t1", 250); err != nil {
		t.Fatalf("seed: %v", err)
	}
	gov := &governance{ledger: ledger}
	breaker := circuit.NopBreaker{}
	admin := buildAdmin(testConfig("127.0.0.1:0"), gov, breaker, []string{"u1"})

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

// TestStartReconcilerStartup pins that a syntactically valid DSN
// assembles the snapshot store and arms the worker without blocking.
// TestReconcileAndExpireHooks covers the metric hooks the workers use.
func TestReconcileAndExpireHooks(t *testing.T) {
	t.Parallel()
	metrics := obs.NewMetrics()
	expiredHook(metrics)(3)
	driftHook(metrics)(quota.TenantDrift{TenantID: "t", Drift: -2})

	var rendered strings.Builder
	if err := metrics.Render(&rendered); err != nil {
		t.Fatalf("render: %v", err)
	}
	for _, want := range []string{
		"breakwater_quota_reconciliation_error 1",
		"breakwater_quota_reservation_expired_total 1",
	} {
		if !strings.Contains(rendered.String(), want) {
			t.Errorf("exposition missing %q:\n%s", want, rendered.String())
		}
	}
}

// seedLedgerFailed wraps a ledger whose EnsureBalance always fails.
type seedLedgerFailed struct {
	quota.Ledger
	err error
}

func (l seedLedgerFailed) EnsureBalance(context.Context, string, int64) (bool, error) {
	return false, l.err
}

// seedLedgerOpaque hides EnsureBalance: to seedBalances it is a ledger
// without provisioning support, which must be skipped silently.
type seedLedgerOpaque struct {
	quota.Ledger
}

func TestSeedBalancesEdgeLedgers(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.DiscardHandler)
	identity, err := auth.NewStatic(auth.StaticConfig{
		Tiers:   []auth.StaticTier{{ID: "free", MonthlyQuota: 100}},
		Tenants: []auth.StaticTenant{{ID: "t", Name: "T", Tier: "free", Keys: []string{"k"}}},
	})
	if err != nil {
		t.Fatalf("identity: %v", err)
	}

	// A failing seeder is logged, never panics.
	seedBalances(context.Background(), identity,
		seedLedgerFailed{Ledger: quota.NewMemory(), err: errors.New("redis down")}, logger)

	// A ledger without provisioning support is skipped.
	seedBalances(context.Background(), identity,
		seedLedgerOpaque{Ledger: quota.NewMemory()}, logger)
}

// TestStartReconcilerRejectsBadDSN covers the startup error branch.
func TestStartReconcilerRejectsBadDSN(t *testing.T) {
	t.Parallel()
	cfg := testConfig("127.0.0.1:0")
	cfg.Postgres.DSN = "not a valid dsn"
	cfg.ReconcileInterval = time.Hour
	if err := startReconciler(context.Background(), cfg, nil, []string{"t"}, obs.NewMetrics(), slog.New(slog.DiscardHandler)); err == nil {
		t.Fatal("an invalid DSN must fail the snapshot store assembly")
	}
}

func TestStartReconcilerStartup(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	redisLedger := quota.NewRedis(rdb)
	cfg := testConfig("127.0.0.1:0")
	cfg.Postgres.DSN = "postgres://breakwater:breakwater@127.0.0.1:1/db" // unreachable, but valid
	cfg.ReconcileInterval = time.Hour

	// The worker is started against a cancelled context so the test ends
	// immediately: assembly is what is under test here.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := startReconciler(ctx, cfg, redisLedger, []string{"t"}, obs.NewMetrics(), slog.New(slog.DiscardHandler)); err != nil {
		t.Fatalf("startReconciler: %v", err)
	}
}

func TestPublishLogDropsSyncsCounter(t *testing.T) {
	t.Parallel()
	var out drainingWriter
	logger := obs.NewLogger(&out, 1)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = logger.Close(ctx)
	}()

	metrics := obs.NewMetrics()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	publishLogDrops(ctx, metrics, logger, 5*time.Millisecond)
	logger.Record(obsEntry()) // capacity 1: the next record drops one
	logger.Record(obsEntry())

	// The publisher syncs on its ticker; poll the rendered exposition.
	var rendered strings.Builder
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		rendered.Reset()
		_ = metrics.Render(&rendered)
		if strings.Contains(rendered.String(), "breakwater_logs_dropped_total 1") {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("logs_dropped counter was never synced")
}

// drainingWriter consumes every write so the logger drains happily.
type drainingWriter struct{}

func (drainingWriter) Write(p []byte) (int, error) { return len(p), nil }

func obsEntry() obs.Entry { return obs.Entry{Status: 200} }
