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
 *   shutdown ordering
 *
 * This root stays the only place that knows concrete implementations;
 * every wire-up decision (which backend, which stages) is made here.
 */
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/daftpunkwav/breakwater/internal/circuit"
	"github.com/daftpunkwav/breakwater/internal/config"
	"github.com/daftpunkwav/breakwater/internal/limiter"
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

	// Governance backends: Redis when configured, in-memory otherwise
	// (development and evidence runs). Memory mode keeps the exact same
	// pipeline semantics with process-local state.
	gov, err := newGovernance(ctx, cfg)
	if err != nil {
		logger.Error("assemble governance backends", "error", err)
		os.Exit(1)
	}
	defer gov.close()

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

	var stages []pipeline.Middleware
	if authStore != nil {
		stages = append(stages,
			pipeline.CarrierStage(),
			pipeline.AuthStage(authStore),
			limiter.Middleware(gov.limiter),
			quota.Middleware(gov.ledger),
		)
		if gov.sweepTarget != nil {
			quota.StartSweeper(ctx, gov.sweepTarget, sweepInterval, nil)
		}
		if staticIdentity != nil {
			seedBalances(ctx, staticIdentity, gov.ledger, logger)
		}
	} else {
		stages = append(stages, pipeline.CarrierStage())
		logger.Warn("no identity configured: running without governance stages")
	}

	bindings, err := buildBindings(cfg.Upstreams)
	if err != nil {
		logger.Error("build upstream adapters", "error", err)
		os.Exit(1)
	}
	rt, err := router.NewPriority(bindings, router.WithBreaker(circuit.NopBreaker{}))
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
	}, retry.NewBudget(cfg.Retry.BudgetMaxInFlight))

	completions := pipeline.Chain(stages...)(server.NewCompletions(rt, relayer))

	srv := server.New(server.Options{
		Addr:          cfg.Server.Addr,
		ShutdownGrace: cfg.Server.ShutdownGrace,
		Completions:   completions,
		Readiness:     gov.readiness,
	})

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
