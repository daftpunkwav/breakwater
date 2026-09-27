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
	"errors"
	"fmt"
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
	"github.com/daftpunkwav/breakwater/internal/httpserver"
	"github.com/daftpunkwav/breakwater/internal/insights"
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

	// The monitoring and assessment store: one row per finished
	// request, batched into PostgreSQL; disabled without a DSN.
	recorder, err := newInsights(ctx, cfg, logger)
	if err != nil {
		return err
	}
	defer recorder.close()
	sink := combinedSink{file: accessLog.sink, insights: recorder.store}

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
	authStore, staticIdentity, identityAdmin, closeIdentity, identityReady, err := newAuthStore(ctx, cfg)
	if err != nil {
		return err
	}
	defer closeIdentity()

	// The governance stage template, shared by every client format; the
	// format stage in front pins which wire parses and renders.
	governance := []pipeline.Middleware{
		pipeline.ObservationStage(metrics, sink),
	}
	if authStore != nil {
		// One gate for the process lifetime: its slot map IS the
		// in-flight state.
		concurrencyGate := limiter.NewConcurrency()
		governance = append(governance,
			pipeline.AuthStage(authStore),
			// Tier model authorization before every spend and before the
			// cache: the cache key is the request body alone, so a replay
			// must never bypass the tier's allow/deny decision.
			pipeline.ModelAuthzStage(),
			// Concurrency sits before the rate limit: a request rejected
			// for concurrency must not consume rate budget or quota.
			limiter.ConcurrencyMiddleware(concurrencyGate, metrics),
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
		// Quota reconciliation (PRD Q6) needs all three: the Redis hot
		// ledger to read, PostgreSQL to persist snapshots into, and the
		// interval armed. The tenant list comes from the system of
		// record — the static set is configuration (no DSN, no snapshot
		// store), so PostgreSQL identity mode is the reconcilable one.
		reconcileArmed := gov.snapshotSource != nil && identityAdmin != nil && cfg.Postgres.DSN != ""
		switch {
		case reconcileArmed && cfg.ReconcileInterval > 0:
			users, err := identityAdmin.Users(ctx)
			if err != nil {
				return fmt.Errorf("reconciler: list tenants: %w", err)
			}
			tenants := make([]string, 0, len(users))
			for _, u := range users {
				tenants = append(tenants, u.ID)
			}
			if err := startReconciler(ctx, cfg, gov.snapshotSource, tenants, metrics, logger); err != nil {
				return err
			}
		case cfg.ReconcileInterval > 0:
			// The interval is set but a prerequisite is missing: say so
			// at startup instead of leaving an armed-looking config
			// silently inert.
			logger.Warn("quota reconciliation disabled: it needs Redis for the hot ledger and a reachable PostgreSQL identity store for snapshots",
				"interval", cfg.ReconcileInterval, "redis", gov.snapshotSource != nil,
				"postgres", cfg.Postgres.DSN != "")
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
	routingSwitch := router.NewSwitch(knownModels(cfg.Upstreams), upstreamIDs(cfg.Upstreams),
		router.WithWildcardModels(wildcardServed(cfg.Upstreams)))
	if err := validateModelNames(cfg.Upstreams, cfg.Fallbacks, cfg.ContextLimits); err != nil {
		return err
	}
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
		relay.WithUpstreamFatalHook(autoDisableHook(routingSwitch, metrics, logger)),
	)

	// One chain per client format, one route per chain. The fallback
	// chains and the context ceilings ride on every format: both are
	// model-level decisions, and the relay applies them per request.
	inference := make(map[protocol.Format]http.Handler, len(formats))
	for _, format := range formats {
		stages := append([]pipeline.Middleware{
			pipeline.CarrierStage(),
			pipeline.RequestIDStage(),
			pipeline.FormatStage(format),
		}, governance...)
		// Innermost: a handler panic renders as a counted 500 instead of
		// a killed connection the observation stage would misread as a
		// client disconnect.
		stages = append(stages, pipeline.RecoveryStage())
		inference[format] = pipeline.Chain(stages...)(server.NewInference(format, rt, relayer,
			server.WithFallbacks(cfg.Fallbacks),
			server.WithContextLimits(cfg.ContextLimits)))
	}

	adminOpts := []server.AdminOption{
		server.WithRouting(routingSwitch),
		server.WithIdentityStore(identityAdmin),
	}
	if recorder.store != nil {
		// Install the reporter only with a live store: a nil *PGStore
		// inside the Reporter interface is not a nil interface, and the
		// endpoint's nil check would never fire.
		adminOpts = append(adminOpts, server.WithInsights(recorder.store))
	}

	// Upstream adapters by id, and the probe-capable subset: the admin
	// reset/probe endpoints and the recovery loop all key off these.
	adapters := make(map[string]upstream.Upstream, len(bindings))
	for _, b := range bindings {
		adapters[b.Upstream.ID()] = b.Upstream
	}
	probes := make(map[string]upstream.Upstream, len(adapters))
	for _, c := range cfg.Upstreams {
		if c.ProbeURL != "" {
			if u, ok := adapters[c.ID]; ok {
				probes[c.ID] = u
			}
		}
	}

	srv := server.New(server.Options{
		Addr:          cfg.Server.Addr,
		ShutdownGrace: cfg.Server.ShutdownGrace,
		Inference:     inference,
		Metrics:       metricsHandler(metrics),
		Admin:         buildAdmin(cfg, gov, breaker, metrics, upstreamIDs(cfg.Upstreams), adapters, probes, adminOpts...),
		Readiness:     mergeReadiness(gov.readiness, identityReady),
		Models:        discoverableModels(cfg.Upstreams),
		Version:       version,
	})

	publishLogDrops(ctx, metrics, accessLog.logger, recorder.store, dropPublishInterval)
	// Active recovery probing: upstreams taken out of rotation (auto
	// disabled, breaker ejected) are asked periodically whether they
	// are back; only those with a configured probe_url can be asked.
	startRecovery(ctx, cfg.Probe.Interval, cfg.Probe.Timeout, cfg.Probe.Threshold,
		breaker, routingSwitch, probes, metrics, logger)

	logger.Info("gateway starting",
		"version", version,
		"addr", cfg.Server.Addr,
		"upstreams", len(cfg.Upstreams),
		"governance", gov.mode,
		"admin_auth", adminAuthState(cfg.Security.AdminToken))
	if err := srv.Run(ctx); err != nil {
		if errors.Is(err, httpserver.ErrDrainTimeout) {
			// A shutdown past its grace window with requests still in
			// flight is a shutdown with a warning, not a failure: exiting
			// non-zero here would turn every rolling restart that carries
			// a long stream into a failed unit. The stragglers were cut;
			// say so loudly and stop cleanly.
			accessLog.close()
			logger.Warn("shutdown grace expired with requests in flight", "grace", cfg.Server.ShutdownGrace)
			logger.Info("gateway stopped")
			return nil
		}
		// The deferred close would still run on the happy path, but a
		// serve error must flush the observation queue before the caller
		// turns the error into an exit code (invariant I8).
		accessLog.close()
		return err
	}
	logger.Info("gateway stopped")
	return nil
}

// adminAuthState names the admin surface's credential posture for the
// startup log: an empty token leaves the surface open, and an operator
// must be able to tell a deliberate dev deployment from a dropped
// secret at a glance.
func adminAuthState(token string) string {
	if token == "" {
		return "disabled (admin surface is open)"
	}
	return "bearer token"
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

// publishLogDrops keeps both sink drop counters in sync with their
// owners' internal counts: the access log's capacity drops and the
// insights store's dropped records. A silently missing insight record
// must be as visible as a silently missing log line.
func publishLogDrops(ctx context.Context, m *obs.Metrics, l *obs.Logger, s *insights.PGStore, every time.Duration) {
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
				if s != nil {
					m.SetInsightsDropped(s.Dropped())
				}
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

// autoDisableHook adapts the relay's fatal-condition reports to the
// routing switch: a fatally broken upstream leaves rotation with the
// reason recorded, counted, and logged — once per transition.
func autoDisableHook(routingSwitch *router.Switch, metrics *obs.Metrics, logger *slog.Logger) func(upstreamID, reason string) {
	return func(upstreamID, reason string) {
		newly, err := routingSwitch.AutoDisableUpstream(upstreamID, reason)
		if err != nil {
			logger.Warn("upstream auto-disable rejected", "upstream", upstreamID, "reason", reason, "error", err)
			return
		}
		if !newly {
			return
		}
		metrics.UpstreamAutoDisabled(upstreamID, reason)
		logger.Warn("upstream auto-disabled, dropping from rotation",
			"upstream", upstreamID, "reason", reason)
	}
}

// validateModelNames fails startup when a fallback chain or a context
// ceiling names a model no configured upstream serves: a typo'd name
// must refuse to boot, never fire silently or filter silently. The
// served check mirrors the router's own resolution semantics — an
// exact name or a wildcard binding serves a model.
func validateModelNames(cfgs []config.Upstream, fallbacks map[string][]string, limits map[string]int64) error {
	check := func(model, kind string) error {
		if servesModel(cfgs, model) {
			return nil
		}
		return fmt.Errorf("config: %s %q names no configured client-facing model", kind, model)
	}
	for model, chain := range fallbacks {
		if err := check(model, "fallback key"); err != nil {
			return err
		}
		for _, f := range chain {
			if err := check(f, "fallback target"); err != nil {
				return err
			}
		}
	}
	for model := range limits {
		if err := check(model, "context limit"); err != nil {
			return err
		}
	}
	return nil
}

// servesModel reports whether the router would resolve the model: an
// upstream lists it exactly, or lists the wildcard that serves
// everything.
func servesModel(cfgs []config.Upstream, model string) bool {
	if model == "*" {
		return false // the wildcard is not itself a requestable model
	}
	for _, c := range cfgs {
		for _, m := range clientModels(c.Models) {
			if m == model || m == "*" {
				return true
			}
		}
	}
	return false
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

// knownModels lists the concrete client-facing model names the
// configuration serves, for the routing switch's typo protection. The
// wildcard is not a model: disabling it would read as enabled for
// every concrete request name, so it never enters the switch. Two
// other consumers need different views of the same list — model
// validation asks servesModel (which knows the wildcard serves any
// name), discovery asks discoverableModels (which reports the
// wildcard as the catch-all model it is).
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

// wildcardServed reports whether any binding serves every model via
// the "*" wildcard.
func wildcardServed(cfgs []config.Upstream) bool {
	for _, c := range cfgs {
		for _, m := range clientModels(c.Models) {
			if m == "*" {
				return true
			}
		}
	}
	return false
}

// discoverableModels lists what GET /v1/models advertises: every
// concrete client-facing name, plus the wildcard itself when a binding
// serves it — the honest description of a catch-all deployment.
func discoverableModels(cfgs []config.Upstream) []string {
	names := knownModels(cfgs)
	if wildcardServed(cfgs) {
		names = append(names, "*")
	}
	return names
}
