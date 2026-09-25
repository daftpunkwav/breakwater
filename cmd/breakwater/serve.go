/**
 * @file serve
 * @description The gateway's assembly and lifecycle: everything between
 * a valid configuration and a serving process.
 *
 * Responsibilities:
 * - Assemble observation, governance backends, the identity store, the
 *   pipeline stages, the router, the relay engine and the HTTP surface
 * - Own the run lifecycle: signal handling, background workers and
 *   shutdown ordering (access log drains before exit, invariant I8)
 * - Return errors instead of exiting, so the whole path is testable
 *
 * This is the composition heart of the binary: every wire-up decision
 * (which backend, which stages) is made here and nowhere else.
 */
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
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

// serve assembles the gateway from cfg and serves it until ctx or a
// process signal ends the run.
func serve(ctx context.Context, cfg config.Config, logger *slog.Logger, version string) error {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Observation first: every stage records into the same registry.
	metrics := obs.NewMetrics()
	accessLog, err := newAccessLog(cfg, logger)
	if err != nil {
		return err
	}
	defer accessLog.close()

	// Governance backends: Redis when configured, in-memory otherwise
	// (development and evidence runs). Memory mode keeps the exact same
	// pipeline semantics with process-local state.
	gov, err := newGovernance(ctx, cfg)
	if err != nil {
		return err
	}
	defer gov.close()

	breaker := buildBreaker(cfg.Circuit, metrics)

	// Identity: PostgreSQL system of record when a DSN is configured,
	// the static identity set otherwise. Either way the steady state
	// resolves through the process-local LRU. Identity configuration is
	// what arms the governance pipeline.
	authStore, staticIdentity, closeIdentity, identityReady, err := newAuthStore(ctx, cfg)
	if err != nil {
		return err
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
			quota.StartSweeper(ctx, gov.sweepTarget, sweepInterval, expiredHook(metrics))
		}
		if staticIdentity != nil && gov.snapshotSource != nil && cfg.Postgres.DSN != "" && cfg.ReconcileInterval > 0 {
			if err := startReconciler(ctx, cfg, gov.snapshotSource, staticIdentity.Tenants(), metrics, logger); err != nil {
				return err
			}
		}
		if staticIdentity != nil {
			seedBalances(ctx, staticIdentity, gov.ledger, logger)
		}
	} else {
		logger.Warn("no identity configured: running without governance stages")
	}

	bindings, err := buildBindings(cfg.Upstreams)
	if err != nil {
		return err
	}
	// Runtime routing controls and measured-performance tracking: the
	// switch gates eligibility (admin API), the tracker only orders
	// candidates when the latency strategy is on.
	strategy, err := router.ParseStrategy(cfg.Routing.Strategy)
	if err != nil {
		return err
	}
	routingSwitch := router.NewSwitch(knownModels(cfg.Upstreams), upstreamIDs(cfg.Upstreams))
	tracker := router.NewTracker()
	rt, err := router.NewPriority(bindings,
		router.WithBreaker(breaker),
		router.WithSwitch(routingSwitch),
		router.WithStrategy(strategy),
		router.WithTracker(tracker))
	if err != nil {
		return err
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
		relay.WithUpstreamObserver(trackerObserver{tracker}),
	)

	// One chain per client format, one route per chain.
	inference := make(map[protocol.Format]http.Handler, len(formats))
	for _, format := range formats {
		stages := append([]pipeline.Middleware{
			pipeline.CarrierStage(),
			pipeline.RequestIDStage(),
			pipeline.FormatStage(format),
		}, governance...)
		inference[format] = pipeline.Chain(stages...)(server.NewInference(format, rt, relayer))
	}

	srv := server.New(server.Options{
		Addr:          cfg.Server.Addr,
		ShutdownGrace: cfg.Server.ShutdownGrace,
		Inference:     inference,
		Metrics:       metricsHandler(metrics),
		Admin: buildAdmin(cfg, gov, breaker, upstreamIDs(cfg.Upstreams),
			server.WithRouting(routingSwitch)),
		Readiness: mergeReadiness(gov.readiness, identityReady),
		Version:   version,
	})

	publishLogDrops(ctx, metrics, accessLog.logger, dropPublishInterval)

	logger.Info("gateway starting",
		"version", version,
		"addr", cfg.Server.Addr,
		"upstreams", len(cfg.Upstreams),
		"governance", gov.mode)
	if err := srv.Run(ctx); err != nil {
		// The deferred close would still run on the happy path, but a
		// serve error must flush the observation queue before the caller
		// turns the error into an exit code (invariant I8).
		accessLog.close()
		return err
	}
	logger.Info("gateway stopped")
	return nil
}

// startReconciler wires the quota reconciliation protocol (PRD Q6):
// the Redis hot ledger is snapshotted into PostgreSQL on an interval
// and consecutive snapshots must satisfy the balance identity.
func startReconciler(ctx context.Context, cfg config.Config, source quota.SnapshotSource, tenants []string, metrics *obs.Metrics, logger *slog.Logger) error {
	store, err := quota.NewPGSnapshots(ctx, cfg.Postgres.DSN)
	if err != nil {
		return err
	}
	go func() {
		<-ctx.Done()
		store.Close()
	}()

	reconciler := quota.NewReconciler(source, tenants, store)
	quota.StartReconciler(ctx, reconciler, cfg.ReconcileInterval, driftHook(metrics))
	logger.Info("quota reconciliation enabled",
		"interval", cfg.ReconcileInterval, "tenants", len(tenants))
	return nil
}

// expiredHook counts reclaimed lease batches on the metrics.
func expiredHook(metrics *obs.Metrics) func(int) {
	return func(int) { metrics.QuotaExpired() }
}

// driftHook counts detected ledger drifts on the metrics.
func driftHook(metrics *obs.Metrics) func(quota.TenantDrift) {
	return func(quota.TenantDrift) { metrics.QuotaReconciliationError() }
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
func publishLogDrops(ctx context.Context, m *obs.Metrics, l *obs.Logger, every time.Duration) {
	if l == nil || every <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(every)
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

// trackerObserver adapts the router's tracker to the relay's outcome
// port: one exchange latency report per attempt.
type trackerObserver struct{ t *router.Tracker }

func (a trackerObserver) ObserveUpstream(upstreamID string, latency time.Duration, failed bool) {
	a.t.Record(upstreamID, latency, failed)
}

// buildBindings turns configured upstreams into ordered router bindings,
// passing each adapter its "client=real" model rewrites.
func buildBindings(cfgs []config.Upstream) ([]router.Binding, error) {
	bindings := make([]router.Binding, 0, len(cfgs))
	for _, c := range cfgs {
		adapter, err := upstream.NewOpenAI(upstream.OpenAIConfig{
			ID:       c.ID,
			BaseURL:  c.BaseURL,
			APIKey:   c.APIKey,
			ProbeURL: c.ProbeURL,
			ModelMap: upstream.ParseModelMap(c.Models),
		})
		if err != nil {
			return nil, err
		}
		bindings = append(bindings, router.Binding{Models: clientModels(c.Models), Upstream: adapter})
	}
	return bindings, nil
}

// clientModels strips the "client=real" rewrites down to the
// client-facing names the router resolves.
func clientModels(models []string) []string {
	names := make([]string, 0, len(models))
	for _, m := range models {
		if client, _, ok := strings.Cut(m, "="); ok && client != "" {
			names = append(names, client)
			continue
		}
		names = append(names, m)
	}
	return names
}

// knownModels lists every client-facing model name the configuration
// serves, for the routing switch's typo protection. The wildcard is
// not a model: disabling it would read as enabled for every concrete
// request name, so it never enters the switch.
func knownModels(cfgs []config.Upstream) []string {
	seen := make(map[string]struct{})
	var names []string
	for _, c := range cfgs {
		for _, m := range clientModels(c.Models) {
			if m == "*" {
				continue
			}
			if _, ok := seen[m]; ok {
				continue
			}
			seen[m] = struct{}{}
			names = append(names, m)
		}
	}
	return names
}
