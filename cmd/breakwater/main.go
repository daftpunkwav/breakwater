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
	authStore, staticIdentity, closeIdentity, err := newAuthStore(ctx, cfg)
	if err != nil {
		logger.Error("assemble identity store", "error", err)
		os.Exit(1)
	}
	defer closeIdentity()

	stages := []pipeline.Middleware{
		pipeline.CarrierStage(),
		pipeline.ObservationStage(metrics, accessLog.sink),
	}
	if authStore != nil {
		stages = append(stages,
			pipeline.AuthStage(authStore),
			limiter.Middleware(gov.limiter, metrics),
			quota.Middleware(gov.ledger, metrics),
		)
		if cfg.Cache.Enabled {
			stages = append(stages, cache.Middleware(
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
	)

	completions := pipeline.Chain(stages...)(server.NewCompletions(rt, relayer))

	srv := server.New(server.Options{
		Addr:          cfg.Server.Addr,
		ShutdownGrace: cfg.Server.ShutdownGrace,
		Completions:   completions,
		Metrics:       metricsHandler(metrics),
		Admin:         buildAdmin(cfg, gov, breaker, upstreamIDs(cfg.Upstreams)),
		Readiness:     gov.readiness,
	})

	publishLogDrops(ctx, metrics, accessLog.logger)

	logger.Info("gateway starting",
		"addr", cfg.Server.Addr,
		"upstreams", len(cfg.Upstreams),
		"governance", gov.mode)
	if err := srv.Run(ctx); err != nil {
		logger.Error("server terminated", "error", err)
		os.Exit(1)
	}
	logger.Info("gateway stopped")
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

// metricsHandler exposes the Prometheus text exposition.
func metricsHandler(m *obs.Metrics) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		m.Render(w)
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
