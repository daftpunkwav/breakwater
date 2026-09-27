/**
 * @file reconciler_test
 * @description The quota reconciliation worker at the assembly root: the
 * snapshot store's startup error and success paths, and the metric hooks
 * the quota background workers report through.
 */
package main

import (
	"context"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/daftpunkwav/breakwater/internal/obs"
	"github.com/daftpunkwav/breakwater/internal/quota"
	"github.com/redis/go-redis/v9"
)

// TestReconcileAndExpireHooks: the sweeper's reclamation counter and the
// reconciler's drift counter must both reach the exposition, or the
// evidence the operations surface is built on never appears.
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

// TestStartReconcilerStartup pins that a syntactically valid DSN
// assembles the snapshot store and arms the worker without blocking.
func TestStartReconcilerStartup(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	defer mr.Close()
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	redisLedger := quota.NewRedis(rdb, "", 10*time.Minute)
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
