/**
 * @file serve
 * @description The gateway's assembly and lifecycle: everything between
 * a valid configuration and a serving process.
 *
 * Responsibilities:
 * - Assemble observation, governance backends, the identity store, the
 *   pipeline stages, the router, the relay engine and the HTTP surface
 * - Own the run lifecycle: signal handling, background workers and
 *   shutdown ordering (the access log drains before exit)
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
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/daftpunkwav/breakwater/internal/affinity"
	"github.com/daftpunkwav/breakwater/internal/auth"
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

// Identity cache TTLs: how long a revocation or limit change takes to reach live traffic, positive resolutions, and the system-of-record backfill behind them.
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
// process signal ends the run. listener is served when non-nil instead
// of binding cfg.Server.Addr: a caller that already holds a bound
// listener passes it, which removes the reserve-release-rebind race it
// would otherwise face when it needs the address up front. Production
// leaves it nil.
//
// The body is the composition heart of the binary: every wire-up
// decision (which backend, which stages) is made here and nowhere
// else, in five sub-assemblies — observation, governance backends,
// identity, governance stages, the routing plane — followed by the
// run lifecycle.
func serve(ctx context.Context, cfg config.Config, logger *slog.Logger, version string, listener net.Listener) error {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Observation first: every stage records into the same registry.
	metrics, logs, recorder, sink, err := assembleObservation(ctx, cfg, logger)
	if err != nil {
		return err
	}
	defer logs.close()
	defer recorder.close()

	// Governance backends: Redis when configured, in-memory otherwise
	// (development and evidence runs). Memory mode keeps the exact same
	// pipeline semantics with process-local state.
	gov, err := newGovernanceBackends(ctx, cfg)
	if err != nil {
		return err
	}
	defer gov.close()
	breaker := buildBreaker(cfg.Circuit, metrics)

	// Identity: PostgreSQL system of record when a DSN is configured,
	// the static identity set otherwise. Either way the steady state
	// resolves through the process-local LRU. Identity configuration is
	// what arms the governance pipeline.
	identity, err := newAuthStore(ctx, cfg)
	if err != nil {
		return err
	}
	defer identity.close()

	governance, err := buildGovernance(ctx, cfg, gov, metrics, sink, identity, logger)
	if err != nil {
		return err
	}

	routing, err := assembleRoutingPlane(cfg, breaker, metrics, governance, identity.admin, recorder.store, logger)
	if err != nil {
		return err
	}

	srv := server.New(server.Options{
		Addr:          cfg.Server.Addr,
		ShutdownGrace: cfg.Server.ShutdownGrace,
		Listener:      listener,
		Inference:     routing.inference,
		Metrics:       metricsHandler(metrics),
		Admin: buildAdmin(cfg, adminBindings{
			gov:         gov,
			breaker:     breaker,
			metrics:     metrics,
			upstreamIDs: upstreamIDs(cfg.Upstreams),
			adapters:    routing.adapters,
			probes:      routing.probes,
		}, routing.adminOptions...),
		Readiness: mergeReadiness(gov.readiness, identity.ready),
		Models:    discoverableModels(cfg.Upstreams),
		Version:   version,
	})

	publishLogDrops(ctx, metrics, logs.logger, recorder.store, dropPublishInterval)
	// Active recovery probing: upstreams taken out of rotation (auto
	// disabled, breaker ejected) are asked periodically whether they
	// are back; only those with a configured probe_url can be asked.
	startRecovery(ctx, cfg.Probe.Interval, cfg.Probe.Timeout, cfg.Probe.BackoffMax, cfg.Probe.Threshold,
		breaker, routing.routeSwitch, routing.probes, metrics, logger)

	logger.Info("gateway starting",
		"version", version,
		"addr", cfg.Server.Addr,
		"upstreams", len(cfg.Upstreams),
		"governance", gov.mode,
		"admin_auth", adminAuthState(cfg.Security.AdminToken),
		"lease_ttl", cfg.LeaseTTL())
	if cfg.Redis.Addr != "" && redisNamespace(cfg) == "" {
		// Sharing one Redis instance between deployments without a
		// namespace means sharing tenant balances: one environment
		// spends the other's money, and its sweeper refunds leases the
		// other is still serving. Nothing else can detect that at
		// startup, so say it where an operator will actually see it.
		logger.Warn("redis keys are not namespaced: this gateway must be the only deployment on " +
			cfg.Redis.Addr + ", or set " + config.EnvRedisNamespace)
	}
	if err := srv.Run(ctx); err != nil {
		if errors.Is(err, httpserver.ErrDrainTimeout) {
			// A shutdown past its grace window with requests still in
			// flight is a shutdown with a warning, not a failure: exiting
			// non-zero here would turn every rolling restart that carries
			// a long stream into a failed unit. The stragglers were cut;
			// say so loudly and stop cleanly.
			logs.close()
			logger.Warn("shutdown grace expired with requests in flight", "grace", cfg.Server.ShutdownGrace)
			logger.Info("gateway stopped")
			return nil
		}
		// The deferred close would still run on the happy path, but a
		// serve error must flush the observation queue before the caller
		// turns the error into an exit code.
		logs.close()
		return err
	}
	logger.Info("gateway stopped")
	return nil
}

// buildGovernance arms the governance pipeline: the stage list shared
// by every client format (the format stage in front pins which wire
// parses and renders), plus the background workers the configuration
// arms — the lease sweeper, the quota reconciler and static-balance
// seeding. The returned error fails startup only where a silently
// inert governance would be worse than a failed one.
func buildGovernance(ctx context.Context, cfg config.Config, gov *governanceBackends, metrics *obs.Metrics, sink combinedSink, identity identityAssembly, logger *slog.Logger) ([]pipeline.Middleware, error) {
	governance := []pipeline.Middleware{
		pipeline.ObservationStage(metrics, sink),
	}
	if identity.store == nil {
		logger.Warn("no identity configured: running without governance stages")
		return governance, nil
	}
	// One gate for the process lifetime: its slot map IS the
	// in-flight state.
	concurrencyGate := limiter.NewConcurrency()
	governance = append(governance,
		pipeline.AuthStage(identity.store),
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
			cfg.RequestCeiling(),
		))
	}
	if gov.sweepTarget != nil {
		quota.StartSweeper(ctx, gov.sweepTarget, sweepInterval, expiredHook(metrics))
	}
	// Quota reconciliation needs all three: the Redis hot
	// ledger to read, PostgreSQL to persist snapshots into, and the
	// interval armed. The tenant list comes from the system of
	// record — the static set is configuration (no DSN, no snapshot
	// store), so PostgreSQL identity mode is the reconcilable one.
	reconcileArmed := gov.snapshotSource != nil && identity.admin != nil && cfg.Postgres.DSN != ""
	switch {
	case reconcileArmed && cfg.ReconcileInterval > 0:
		users, err := identity.admin.Users(ctx)
		if err != nil {
			return nil, fmt.Errorf("reconciler: list tenants: %w", err)
		}
		tenants := make([]string, 0, len(users))
		for _, u := range users {
			tenants = append(tenants, u.ID)
		}
		if err := startReconciler(ctx, cfg, gov.snapshotSource, tenants, metrics, logger); err != nil {
			return nil, err
		}
	case cfg.ReconcileInterval > 0:
		// The interval is set but a prerequisite is missing: say so
		// at startup instead of leaving an armed-looking config
		// silently inert.
		logger.Warn("quota reconciliation disabled: it needs Redis for the hot ledger and a reachable PostgreSQL identity store for snapshots",
			"interval", cfg.ReconcileInterval, "redis", gov.snapshotSource != nil,
			"postgres", cfg.Postgres.DSN != "")
	}
	if identity.static != nil {
		seedBalances(ctx, identity.static, gov.ledger, logger)
	}
	return governance, nil
}

// routingPlane bundles the wired routing surface: the per-format
// inference chains with the router and relay engine joined in, the
// switch the recovery loop and the admin API drive, the admin options
// that hang the routing controls on the admin surface, and the adapter
// maps the admin endpoints key off.
type routingPlane struct {
	routeSwitch  *router.Switch
	inference    map[protocol.Format]http.Handler
	adminOptions []server.AdminOption
	adapters     map[string]upstream.Upstream
	probes       map[string]upstream.Upstream
}

// assembleRoutingPlane resolves the configured upstreams into ordered
// router bindings, the routing switch, the retry budget and the relay
// engine, and joins them into one inference chain per client format.
func assembleRoutingPlane(cfg config.Config, breaker circuit.Breaker, metrics *obs.Metrics, governance []pipeline.Middleware, identityAdmin auth.AdminStore, insightsStore *insights.PGStore, logger *slog.Logger) (routingPlane, error) {
	bindings, rings, err := buildBindings(cfg.Upstreams)
	if err != nil {
		return routingPlane{}, err
	}
	// Runtime routing controls and measured-performance tracking: the
	// switch gates eligibility (admin API), the tracker only orders
	// candidates when the latency strategy is on.
	strategy, err := router.ParseStrategy(cfg.Routing.Strategy)
	if err != nil {
		return routingPlane{}, err
	}
	routingSwitch := router.NewSwitch(knownModels(cfg.Upstreams), upstreamIDs(cfg.Upstreams),
		router.WithWildcardModels(wildcardServed(cfg.Upstreams)),
		router.WithOnUpstreamEnable(reviveRing(rings, logger)))
	// An upstream returning to rotation — operator enable or lifted
	// auto disable — carries its credential ring back to full strength:
	// the same decision that lifts the disable lifts what the fatal
	// conditions retired inside it.
	if err := validateModelNames(cfg.Upstreams, cfg.Fallbacks, cfg.ContextLimits); err != nil {
		return routingPlane{}, err
	}
	tracker := router.NewTracker()
	priority, err := router.NewPriority(bindings,
		router.WithBreaker(breaker),
		router.WithSwitch(routingSwitch),
		router.WithStrategy(strategy),
		router.WithTracker(tracker))
	if err != nil {
		return routingPlane{}, err
	}

	// The retry budget: a fixed cap by default, or a share of live
	// traffic when a percentage is configured — retries then scale
	// with the requests that justify them instead of one static number.
	budget := retry.NewBudget(cfg.Retry.BudgetMaxInFlight)
	if cfg.Retry.BudgetPercent > 0 {
		share, err := retry.NewShareBudget(cfg.Retry.BudgetPercent, cfg.Retry.BudgetMinInFlight, metrics.Inflight)
		if err != nil {
			return routingPlane{}, err
		}
		budget = share
	}

	relayer := relay.New(retry.Policy{
		MaxAttempts:     cfg.Retry.MaxAttempts,
		AttemptTimeout:  cfg.Retry.AttemptTimeout,
		OverallDeadline: cfg.Retry.OverallDeadline,
		BackoffInitial:  cfg.Retry.BackoffInitial,
		BackoffMax:      cfg.Retry.BackoffMax,
	}, budget,
		relay.WithBreaker(breaker),
		relay.WithMetrics(metrics),
		relay.WithStreamTimeout(cfg.Retry.StreamTimeout),
		relay.WithStreamIdleTimeout(cfg.Retry.StreamIdleTimeout),
		relay.WithSlowCallThreshold(cfg.Circuit.SlowCallThreshold),
		relay.WithUpstreamBulkhead(limiter.NewConcurrency(), int64(cfg.UpstreamMaxInFlight)),
		relay.WithUpstreamObserver(trackerObserver{tracker}),
		relay.WithUpstreamFatalHook(autoDisableHook(routingSwitch, rings, metrics, logger)),
	)

	// One chain per client format, one route per chain. The fallback
	// chains and the context ceilings ride on every format: both are
	// model-level decisions, and the relay applies them per request.
	// The affinity index joins them: one shared instance across the
	// formats, since the prefixes it remembers belong to the upstreams,
	// not to the client format that delivered them. A non-positive TTL
	// yields nil and the handler keeps the router's order.
	affinityIndex := affinity.NewIndex(cfg.Routing.AffinityTTL, time.Now)
	inference := make(map[protocol.Format]http.Handler, len(formats))
	for _, format := range formats {
		inference[format] = inferenceChain(format, governance,
			server.NewInference(format, priority, relayer,
				server.WithFallbacks(cfg.Fallbacks),
				server.WithContextLimits(cfg.ContextLimits),
				server.WithAffinity(affinityIndex)))
	}

	adminOpts := []server.AdminOption{
		server.WithRouting(routingSwitch),
		server.WithIdentityStore(identityAdmin),
	}
	if insightsStore != nil {
		// Install the reporter only with a live store: a nil *PGStore
		// inside the Reporter interface is not a nil interface, and the
		// endpoint's nil check would never fire.
		adminOpts = append(adminOpts, server.WithInsights(insightsStore))
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

	return routingPlane{
		routeSwitch:  routingSwitch,
		inference:    inference,
		adminOptions: adminOpts,
		adapters:     adapters,
		probes:       probes,
	}, nil
}

// inferenceChain wraps one client format's endpoint in the full request
// chain: the per-request stages first, then the shared governance
// stages, with recovery innermost — inside the observation stage the
// governance list opens with, so a handler panic renders as a counted
// 500 gateway fault instead of the client-disconnect reading a
// header-less end would otherwise get. The mux-level recovery in the
// server's route table covers the routes that carry no observation
// stage; this inner one is the inference chains' own.
func inferenceChain(format protocol.Format, governance []pipeline.Middleware, endpoint http.Handler) http.Handler {
	stages := append([]pipeline.Middleware{
		pipeline.CarrierStage(),
		pipeline.RequestIDStage(),
		pipeline.FormatStage(format),
	}, governance...)
	stages = append(stages, pipeline.RecoveryStage())
	return pipeline.Chain(stages...)(endpoint)
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

// startReconciler wires the quota reconciliation protocol:
// the Redis hot ledger is snapshotted into PostgreSQL on an interval
// and consecutive snapshots must satisfy the balance identity.
func startReconciler(ctx context.Context, cfg config.Config, source quota.SnapshotSource, tenants []string, metrics *obs.Metrics, logger *slog.Logger) error {
	store, err := quota.NewPGSnapshotStore(ctx, cfg.Postgres.DSN)
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

// buildBreaker assembles the breaker for the configured strategy: the
// consecutive machine with its transition hooks, the ratio guard (no
// transitions ever fire, so the denial counter carries that mode's
// signal alone), or the slow-call machine (driven by slow completions,
// observed through the same transition hooks). Extra options pass
// through to the registry (tests inject a clock).
func buildBreaker(cfg config.Circuit, metrics *obs.Metrics, opts ...circuit.Option) circuit.Breaker {
	if !cfg.Enabled {
		return circuit.NopBreaker{}
	}
	if cfg.Strategy == "ratio" {
		return circuit.NewRatioRegistry(circuit.RatioOnDenial(func(id string) {
			metrics.CircuitDenied(id)
		}))
	}
	if cfg.Strategy == "slow-call" {
		return circuit.NewSlowRegistry(circuit.Config{
			Cooldown:     cfg.Cooldown,
			ProbeTimeout: cfg.ProbeTimeout,
			SlowRatio:    cfg.SlowRatio,
		}, circuit.SlowOnTransition(func(id string, _, to circuit.State) {
			switch to {
			case circuit.StateOpen:
				metrics.CircuitOpened(id)
			case circuit.StateHalfOpen:
				metrics.CircuitHalfOpen(id)
			}
			metrics.CircuitState(id, stateValue(to))
		}))
	}
	return circuit.NewRegistry(circuit.Config{
		FailThreshold: cfg.FailThreshold,
		Cooldown:      cfg.Cooldown,
		ProbeTimeout:  cfg.ProbeTimeout,
	}, append([]circuit.Option{circuit.OnTransition(func(id string, _, to circuit.State) {
		switch to {
		case circuit.StateOpen:
			metrics.CircuitOpened(id)
		case circuit.StateHalfOpen:
			metrics.CircuitHalfOpen(id)
		}
		metrics.CircuitState(id, stateValue(to))
	})}, opts...)...)
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
// must be as visible as a silently missing log line. Either sink may be
// absent — the access log is off unless a path is configured — so each
// is published on its own terms and neither gates the other.
func publishLogDrops(ctx context.Context, m *obs.Metrics, l *obs.Logger, s *insights.PGStore, every time.Duration) {
	if l == nil && s == nil || every <= 0 {
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
				if l != nil {
					m.SetLogsDropped(l.Dropped())
				}
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
// credential rings and the routing switch. A fatal exchange convicts
// the credential that served it first: the ring retires it, counted
// and logged once per transition, and the remaining credentials keep
// the upstream in rotation. Only the last living credential's death
// takes the upstream out of rotation — with the reason recorded, the
// metric counted, and the log line emitted, once per transition. A
// report without a usable credential index (an upstream that holds no
// ring, or an exchange that carried none) goes straight to the
// upstream-level disable, exactly as before rings existed.
func autoDisableHook(routingSwitch *router.Switch, rings map[string]upstream.CredentialPool, metrics *obs.Metrics, logger *slog.Logger) func(upstreamID string, credentialIndex int, reason string) {
	return func(upstreamID string, credentialIndex int, reason string) {
		if ring, ok := rings[upstreamID]; ok && credentialIndex >= 0 {
			if newly := ring.RetireCredential(credentialIndex); newly {
				metrics.CredentialRetired(upstreamID, reason)
				logger.Warn("credential retired, the ring keeps the upstream serving",
					"upstream", upstreamID, "credential_index", credentialIndex,
					"reason", reason, "credentials_alive", ring.AliveCredentials())
				if ring.AliveCredentials() > 0 {
					return
				}
			} else if ring.AliveCredentials() > 0 {
				// Already retired by a concurrent report; nothing changed.
				return
			}
		}
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

// reviveRing returns the enable callback for the routing switch: an
// upstream coming back to rotation restores its full credential ring.
func reviveRing(rings map[string]upstream.CredentialPool, logger *slog.Logger) func(id string) {
	return func(id string) {
		ring, ok := rings[id]
		if !ok {
			return
		}
		ring.ReviveCredentials()
		logger.Info("credential ring restored with the upstream back in rotation", "upstream", id)
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
// passing each adapter its "client=real" model rewrites and its merged
// credential ring. The ring map keys the adapters by id for the fatal
// hook and the enable callback; every OpenAI adapter carries one, so a
// missing key means an unknown upstream, never a ring-less adapter.
func buildBindings(cfgs []config.Upstream) ([]router.Binding, map[string]upstream.CredentialPool, error) {
	bindings := make([]router.Binding, 0, len(cfgs))
	rings := make(map[string]upstream.CredentialPool, len(cfgs))
	for _, c := range cfgs {
		adapter, err := upstream.NewOpenAI(upstream.OpenAIConfig{
			ID:       c.ID,
			BaseURL:  c.BaseURL,
			APIKeys:  c.Credentials(),
			ProbeURL: c.ProbeURL,
			ModelMap: upstream.ParseModelMap(c.Models),
		})
		if err != nil {
			return nil, nil, err
		}
		bindings = append(bindings, router.Binding{Models: clientModels(c.Models), Upstream: adapter})
		rings[c.ID] = adapter
	}
	return bindings, rings, nil
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
