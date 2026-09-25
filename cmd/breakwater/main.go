/**
 * @file main
 * @description Composition root of the breakwater gateway binary.
 *
 * Responsibilities:
 * - Load configuration from the environment
 * - Assemble the identity store, governance backends (memory or Redis),
 *   the pipeline stages, the router, the relay engine and the HTTP
 *   surface
 * - Own the process lifecycle: signal handling, background workers and
 *   shutdown ordering (access log drains before exit, invariant I8)
 *
 * This root stays the only place that knows concrete implementations;
 * every wire-up decision (which backend, which stages) is made here.
 */
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/daftpunkwav/breakwater/internal/cache"
	"github.com/daftpunkwav/breakwater/internal/circuit"
	"github.com/daftpunkwav/breakwater/internal/config"
	"github.com/daftpunkwav/breakwater/internal/limiter"
	"github.com/daftpunkwav/breakwater/internal/obs"
	"github.com/daftpunkwav/breakwater/internal/pipeline"
	"github.com/daftpunkwav/breakwater/internal/protocol"
	"github.com/daftpunkwav/breakwater/internal/quota"
	"github.com/daftpunkwav/breakwater/internal/relay"
	"github.com/daftpunkwav/breakwater/internal/retry"
	"github.com/daftpunkwav/breakwater/internal/router"
	"github.com/daftpunkwav/breakwater/internal/server"
	"github.com/daftpunkwav/breakwater/internal/upstream"
)

// Identity cache TTLs: the documented revocation latency.
const (
	authPosTTL = 60 * time.Second
	authNegTTL = 5 * time.Second
	// sweepInterval paces the lease reclaimer.
	sweepInterval = 30 * time.Second
	// dropPublishInterval paces the logs-dropped gauge sync.
	dropPublishInterval = 5 * time.Second
)

// formats are the client-facing surfaces every route serves.
var formats = []protocol.Format{
	protocol.FormatOpenAIChat,
	protocol.FormatOpenAIResponses,
	protocol.FormatAnthropicMessages,
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg, err := config.Load()
	if err != nil {
		logger.Error("load configuration", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Observation first: every stage records into the same registry.
	metrics := obs.NewMetrics()
	accessLog := newAccessLog(cfg, logger)
	defer accessLog.close()

	// Governance backends: Redis when configured, in-memory otherwise
	// (development and evidence runs). Memory mode keeps the exact same
	// pipeline semantics with process-local state.
	gov, err := newGovernance(ctx, cfg)
	if err != nil {
		logger.Error("assemble governance backends", "error", err)
		os.Exit(1)
	}
	defer gov.close()

	breaker := buildBreaker(cfg.Circuit, metrics)

	// Identity: PostgreSQL system of record when a DSN is configured,
	// the static identity set otherwise. Either way the steady state
	// resolves through the process-local LRU. Identity configuration is
	// what arms the governance pipeline.
	authStore, staticIdentity, closeIdentity, identityReady, err := newAuthStore(ctx, cfg)
	if err != nil {
		logger.Error("assemble identity store", "error", err)
		os.Exit(1)
	}
	defer closeIdentity()

	// The governance stage template, shared by every client format; the
	// format stage in front pins which wire parses and renders.
	governance := []pipeline.Middleware{
		pipeline.ObservationStage(metrics, accessLog.sink),
	}
	if authStore != nil {
		governance = append(governance,
			pipeline.AuthStage(authStore),
			limiter.Middleware(gov.limiter, metrics),
			quota.Middleware(gov.ledger, metrics),
		)
		if cfg.Cache.Enabled {
			governance = append(governance, cache.Middleware(
				cache.NewMemory(cache.WithCapacity(cfg.Cache.Capacity)),
				cache.NewFlight(),
				cfg.Cache.TTL,
				metrics,
			))
		}
		if gov.sweepTarget != nil {
			quota.StartSweeper(ctx, gov.sweepTarget, sweepInterval, func(int) {
				metrics.QuotaExpired()
			})
		}
		if staticIdentity != nil && gov.snapshotSource != nil && cfg.Postgres.DSN != "" && cfg.ReconcileInterval > 0 {
			startReconciler(ctx, cfg, gov.snapshotSource, staticIdentity.Tenants(), metrics, logger)
		}
		if staticIdentity != nil {
			seedBalances(ctx, staticIdentity, gov.ledger, logger)
		}
	} else {
		logger.Warn("no identity configured: running without governance stages")
	}

	bindings, err := buildBindings(cfg.Upstreams)
	if err != nil {
		logger.Error("build upstream adapters", "error", err)
		os.Exit(1)
	}
	rt, err := router.NewPriority(bindings, router.WithBreaker(breaker))
	if err != nil {
		logger.Error("build router", "error", err)
		os.Exit(1)
	}

	relayer := relay.New(retry.Policy{
		MaxAttempts:     cfg.Retry.MaxAttempts,
		AttemptTimeout:  cfg.Retry.AttemptTimeout,
		OverallDeadline: cfg.Retry.OverallDeadline,
		BackoffInitial:  cfg.Retry.BackoffInitial,
		BackoffMax:      cfg.Retry.BackoffMax,
	}, retry.NewBudget(cfg.Retry.BudgetMaxInFlight),
		relay.WithBreaker(breaker),
		relay.WithMetrics(metrics),
		relay.WithStreamTimeout(cfg.Retry.StreamTimeout),
	)

	// One chain per client format, one route per chain.
	inference := make(map[protocol.Format]http.Handler, len(formats))
	for _, format := range formats {
		stages := append([]pipeline.Middleware{
			pipeline.CarrierStage(),
			pipeline.FormatStage(format),
		}, governance...)
		inference[format] = pipeline.Chain(stages...)(server.NewInference(format, rt, relayer))
	}

	srv := server.New(server.Options{
		Addr:          cfg.Server.Addr,
		ShutdownGrace: cfg.Server.ShutdownGrace,
		Inference:     inference,
		Metrics:       metricsHandler(metrics),
		Admin:         buildAdmin(cfg, gov, breaker, upstreamIDs(cfg.Upstreams)),
		Readiness:     mergeReadiness(gov.readiness, identityReady),
	})

	publishLogDrops(ctx, metrics, accessLog.logger)

	logger.Info("gateway starting",
		"addr", cfg.Server.Addr,
		"upstreams", len(cfg.Upstreams),
		"governance", gov.mode)
	if err := srv.Run(ctx); err != nil {
		logger.Error("server terminated", "error", err)
		// os.Exit skips deferred calls: flush the observation queue here
		// so the error path keeps invariant I8 too.
		accessLog.close()
		os.Exit(1)
	}
	logger.Info("gateway stopped")
}

// startReconciler wires the quota reconciliation protocol (PRD Q6):
// the Redis hot ledger is snapshotted into PostgreSQL on an interval
// and consecutive snapshots must satisfy the balance identity.
func startReconciler(ctx context.Context, cfg config.Config, source quota.SnapshotSource, tenants []string, metrics *obs.Metrics, logger *slog.Logger) {
	store, err := quota.NewPGSnapshots(ctx, cfg.Postgres.DSN)
	if err != nil {
		logger.Error("assemble quota snapshot store", "error", err)
		os.Exit(1)
	}
	go func() {
		<-ctx.Done()
		store.Close()
	}()

	reconciler := quota.NewReconciler(source, tenants, store)
	quota.StartReconciler(ctx, reconciler, cfg.ReconcileInterval, func(drift quota.TenantDrift) {
		metrics.QuotaReconciliationError()
	})
	logger.Info("quota reconciliation enabled",
		"interval", cfg.ReconcileInterval, "tenants", len(tenants))
}

// buildBreaker assembles the breaker registry with its metric hooks.
func buildBreaker(cfg config.Circuit, metrics *obs.Metrics) circuit.Breaker {
	if !cfg.Enabled {
		return circuit.NopBreaker{}
	}
	return circuit.NewRegistry(circuit.Config{
		FailThreshold: cfg.FailThreshold,
		Cooldown:      cfg.Cooldown,
		ProbeTimeout:  cfg.ProbeTimeout,
	}, circuit.OnTransition(func(id string, _, to circuit.State) {
		switch to {
		case circuit.StateOpen:
			metrics.CircuitOpened(id)
		case circuit.StateHalfOpen:
			metrics.CircuitHalfOpen(id)
		}
		metrics.CircuitState(id, stateValue(to))
	}))
}

// stateValue maps breaker states onto the published gauge.
func stateValue(s circuit.State) float64 {
	switch s {
	case circuit.StateOpen:
		return 2
	case circuit.StateHalfOpen:
		return 1
	default:
		return 0
	}
}

// mergeReadiness combines probes: ready only when every live probe is
// ready. Nil probes drop out; an all-nil set reports ready.
func mergeReadiness(probes ...func() error) func() error {
	var live []func() error
	for _, probe := range probes {
		if probe != nil {
			live = append(live, probe)
		}
	}
	if len(live) == 0 {
		return nil
	}
	return func() error {
		for _, probe := range live {
			if err := probe(); err != nil {
				return err
			}
		}
		return nil
	}
}

// metricsHandler exposes the Prometheus text exposition.
func metricsHandler(m *obs.Metrics) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		// A scrape client that walked away is not a gateway problem.
		_ = m.Render(w)
	})
}

// publishLogDrops keeps the logs-dropped counter in sync with the
// access log's internal counter.
func publishLogDrops(ctx context.Context, m *obs.Metrics, l *obs.Logger) {
	if l == nil {
		return
	}
	go func() {
		ticker := time.NewTicker(dropPublishInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				m.SetLogsDropped(l.Dropped())
			}
		}
	}()
}

// buildBindings turns configured upstreams into ordered router bindings.
func buildBindings(cfgs []config.Upstream) ([]router.Binding, error) {
	bindings := make([]router.Binding, 0, len(cfgs))
	for _, c := range cfgs {
		adapter, err := upstream.NewOpenAI(upstream.OpenAIConfig{
			ID:       c.ID,
			BaseURL:  c.BaseURL,
			APIKey:   c.APIKey,
			ProbeURL: c.ProbeURL,
		})
		if err != nil {
			return nil, err
		}
		bindings = append(bindings, router.Binding{Models: c.Models, Upstream: adapter})
	}
	return bindings, nil
}
