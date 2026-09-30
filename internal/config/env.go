/**
 * @file env
 * @description Environment-based configuration loading with validation.
 *
 * Responsibilities:
 * - Read BREAKWATER_* environment variables
 * - Fall back to defaults and reject invalid values loudly
 */
package config

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// Defaults keep the gateway runnable with zero configuration. They are
// the tuned baseline an operator overrides through the BREAKWATER_*
// variables read below.
const (
	defaultAddr          = ":8080"
	defaultShutdownGrace = 15 * time.Second
	// No default Redis address: memory backends are the zero-config
	// mode; Redis runs only when explicitly configured.
	defaultRedisAddr        = ""
	defaultAccessLogQueue   = 4096
	defaultMaxAttempts      = 3
	defaultAttemptTimeout   = 30 * time.Second
	defaultOverallDeadline  = 60 * time.Second
	defaultBackoffInitial   = 100 * time.Millisecond
	defaultBackoffMax       = 2 * time.Second
	defaultRetryBudgetCap   = 64
	defaultRetryBudgetMin   = 3
	defaultCacheTTL         = 60 * time.Second
	defaultCacheCapacity    = 1024
	defaultCircuitThreshold = 5
	defaultCircuitCooldown  = 30 * time.Second
	defaultCircuitProbe     = 5 * time.Second
	defaultCircuitSlowRatio = 0.5
	defaultStreamTimeout    = 10 * time.Minute
	defaultReconcileEvery   = time.Minute
	defaultProbeInterval    = 30 * time.Second
	defaultProbeTimeout     = 5 * time.Second
	defaultProbeThreshold   = 2
)

const (
	envAddr           = "BREAKWATER_ADDR"
	envShutdownGrace  = "BREAKWATER_SHUTDOWN_GRACE"
	envRedisAddr      = "BREAKWATER_REDIS_ADDR"
	envRedisTLS       = "BREAKWATER_REDIS_TLS"
	envPostgresDSN    = "BREAKWATER_POSTGRES_DSN"
	envAccessLogQueue = "BREAKWATER_ACCESS_LOG_QUEUE_SIZE"
	envUpstreams      = "BREAKWATER_UPSTREAMS"

	// envAllowUnauthenticated is the explicit dev opt-in that permits a
	// deployment Load would otherwise refuse: upstreams without any
	// identity source (an unauthenticated open proxy), or a PostgreSQL
	// identity without an admin token (an open key-minting surface).
	envAllowUnauthenticated = "BREAKWATER_ALLOW_UNAUTHENTICATED"

	// EnvRedisNamespace is exported because the composition root points
	// operators at it by name when the namespace is missing: the message
	// must not carry a second, drifting spelling of the variable.
	EnvRedisNamespace = "BREAKWATER_REDIS_NAMESPACE"

	envQuotaLeaseTTL = "BREAKWATER_QUOTA_LEASE_TTL"

	envRetryMaxAttempts    = "BREAKWATER_RETRY_MAX_ATTEMPTS"
	envRetryAttemptTimeout = "BREAKWATER_RETRY_ATTEMPT_TIMEOUT"
	envRetryOverall        = "BREAKWATER_RETRY_OVERALL_DEADLINE"
	envRetryBackoffInitial = "BREAKWATER_RETRY_BACKOFF_INITIAL"
	envRetryBackoffMax     = "BREAKWATER_RETRY_BACKOFF_MAX"
	envRetryBudget         = "BREAKWATER_RETRY_BUDGET_MAX_IN_FLIGHT"
	envRetryBudgetPercent  = "BREAKWATER_RETRY_BUDGET_PERCENT"
	envRetryBudgetMin      = "BREAKWATER_RETRY_BUDGET_MIN_IN_FLIGHT"
	envStreamTimeout       = "BREAKWATER_STREAM_TIMEOUT"
	envStreamIdleTimeout   = "BREAKWATER_STREAM_IDLE_TIMEOUT"
	envReconcileInterval   = "BREAKWATER_RECONCILE_INTERVAL"
	envIdentity            = "BREAKWATER_IDENTITY"

	envCacheEnabled  = "BREAKWATER_CACHE_ENABLED"
	envCacheTTL      = "BREAKWATER_CACHE_TTL"
	envCacheCapacity = "BREAKWATER_CACHE_CAPACITY"

	envCircuitEnabled    = "BREAKWATER_CIRCUIT_ENABLED"
	envCircuitStrategy   = "BREAKWATER_CIRCUIT_STRATEGY"
	envCircuitThreshold  = "BREAKWATER_CIRCUIT_FAIL_THRESHOLD"
	envCircuitCooldown   = "BREAKWATER_CIRCUIT_COOLDOWN"
	envCircuitProbe      = "BREAKWATER_CIRCUIT_PROBE_TIMEOUT"
	envCircuitSlowRatio  = "BREAKWATER_CIRCUIT_SLOW_RATIO"
	envCircuitSlowThresh = "BREAKWATER_CIRCUIT_SLOW_THRESHOLD"

	envAccessLogPath = "BREAKWATER_ACCESS_LOG_PATH"
	envAdminToken    = "BREAKWATER_ADMIN_TOKEN"
	envRouting       = "BREAKWATER_ROUTING_STRATEGY"
	envInsightsDSN   = "BREAKWATER_INSIGHTS_DSN"

	// The active recovery loop's knobs (cmd/breakwater): these probe
	// auto-disabled upstreams back into rotation. They are not the
	// circuit breaker's own half-open probe, whose timeout is
	// BREAKWATER_CIRCUIT_PROBE_TIMEOUT above.
	envProbeInterval  = "BREAKWATER_PROBE_INTERVAL"
	envProbeTimeout   = "BREAKWATER_PROBE_TIMEOUT"
	envProbeThreshold = "BREAKWATER_PROBE_THRESHOLD"
	envProbeBackoff   = "BREAKWATER_PROBE_BACKOFF_MAX"

	envFallbacks     = "BREAKWATER_FALLBACKS"
	envContextLimits = "BREAKWATER_CONTEXT_LIMITS"
)

// Load reads the configuration from the environment and validates it.
func Load() (Config, error) {
	cfg := Config{
		Server: Server{
			Addr:          envString(envAddr, defaultAddr),
			ShutdownGrace: defaultShutdownGrace,
		},
		Redis: Redis{
			Addr:      envString(envRedisAddr, defaultRedisAddr),
			Namespace: strings.TrimSpace(os.Getenv(EnvRedisNamespace)),
		},
		Postgres: Postgres{
			DSN: strings.TrimSpace(os.Getenv(envPostgresDSN)),
		},
		Identity: strings.TrimSpace(os.Getenv(envIdentity)),
		Obs: Obs{
			AccessLogQueueSize: defaultAccessLogQueue,
			AccessLogPath:      strings.TrimSpace(os.Getenv(envAccessLogPath)),
			InsightsDSN:        strings.TrimSpace(os.Getenv(envInsightsDSN)),
		},
		Security: Security{
			AdminToken: strings.TrimSpace(os.Getenv(envAdminToken)),
		},
		Retry: Retry{
			MaxAttempts:       defaultMaxAttempts,
			AttemptTimeout:    defaultAttemptTimeout,
			OverallDeadline:   defaultOverallDeadline,
			BackoffInitial:    defaultBackoffInitial,
			BackoffMax:        defaultBackoffMax,
			BudgetMaxInFlight: defaultRetryBudgetCap,
			BudgetPercent:     0,
			BudgetMinInFlight: defaultRetryBudgetMin,
			StreamTimeout:     defaultStreamTimeout,
		},
		ReconcileInterval: defaultReconcileEvery,
		Cache: Cache{
			Enabled:  true,
			TTL:      defaultCacheTTL,
			Capacity: defaultCacheCapacity,
		},
		Circuit: Circuit{
			Enabled:       true,
			Strategy:      "consecutive",
			FailThreshold: defaultCircuitThreshold,
			Cooldown:      defaultCircuitCooldown,
			ProbeTimeout:  defaultCircuitProbe,
			SlowRatio:     defaultCircuitSlowRatio,
		},
		Probe: Probe{
			Interval:  defaultProbeInterval,
			Timeout:   defaultProbeTimeout,
			Threshold: defaultProbeThreshold,
		},
		Routing: Routing{
			Strategy: envString(envRouting, "static"),
		},
	}

	var err error
	if cfg.Identity, err = loadIdentitySource(envIdentity, cfg.Identity); err != nil {
		return Config{}, err
	}
	if cfg.Redis.TLS, err = envBool(envRedisTLS, false); err != nil {
		return Config{}, err
	}
	if cfg.Fallbacks, err = envJSONMap[[]string](envFallbacks); err != nil {
		return Config{}, err
	}
	if cfg.ContextLimits, err = envJSONMap[int64](envContextLimits); err != nil {
		return Config{}, err
	}
	if cfg.Server.ShutdownGrace, err = envDuration(envShutdownGrace, cfg.Server.ShutdownGrace); err != nil {
		return Config{}, err
	}
	if cfg.Obs.AccessLogQueueSize, err = envInt(envAccessLogQueue, cfg.Obs.AccessLogQueueSize); err != nil {
		return Config{}, err
	}
	if cfg.Upstreams, err = envJSON[Upstream](envUpstreams); err != nil {
		return Config{}, err
	}
	// Each subsystem owns its parse-and-check rules in one place, in
	// load order; validate enforces the bounds checked after parsing.
	if err := loadRetry(&cfg); err != nil {
		return Config{}, err
	}
	if err := loadQuota(&cfg); err != nil {
		return Config{}, err
	}
	if err := loadCache(&cfg); err != nil {
		return Config{}, err
	}
	if err := loadCircuit(&cfg); err != nil {
		return Config{}, err
	}
	if err := loadProbe(&cfg); err != nil {
		return Config{}, err
	}
	if err := validate(&cfg); err != nil {
		return Config{}, err
	}
	if err := guardDeploymentPosture(cfg); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// loadRetry parses the attempt-loop settings and enforces their
// per-setting bounds in load order.
func loadRetry(cfg *Config) error {
	var err error
	if cfg.Retry.MaxAttempts, err = envInt(envRetryMaxAttempts, cfg.Retry.MaxAttempts); err != nil {
		return err
	}
	if cfg.Retry.AttemptTimeout, err = envDuration(envRetryAttemptTimeout, cfg.Retry.AttemptTimeout); err != nil {
		return err
	}
	if cfg.Retry.OverallDeadline, err = envDuration(envRetryOverall, cfg.Retry.OverallDeadline); err != nil {
		return err
	}
	if cfg.Retry.BackoffInitial, err = envDuration(envRetryBackoffInitial, cfg.Retry.BackoffInitial); err != nil {
		return err
	}
	if cfg.Retry.BackoffMax, err = envDuration(envRetryBackoffMax, cfg.Retry.BackoffMax); err != nil {
		return err
	}
	// Zero is the documented "no cap" for each of these in the attempt
	// loop; a negative value is a typo that would expire every attempt
	// context immediately (or wrap the backoff math) — refuse to boot.
	for _, d := range []struct {
		key string
		val time.Duration
	}{
		{envRetryAttemptTimeout, cfg.Retry.AttemptTimeout},
		{envRetryOverall, cfg.Retry.OverallDeadline},
		{envRetryBackoffInitial, cfg.Retry.BackoffInitial},
		{envRetryBackoffMax, cfg.Retry.BackoffMax},
	} {
		if d.val < 0 {
			return fmt.Errorf("config: %s must not be negative", d.key)
		}
	}
	if cfg.Retry.BudgetMaxInFlight, err = envInt(envRetryBudget, cfg.Retry.BudgetMaxInFlight); err != nil {
		return err
	}
	if cfg.Retry.BudgetPercent, err = envInt(envRetryBudgetPercent, cfg.Retry.BudgetPercent); err != nil {
		return err
	}
	if cfg.Retry.BudgetMinInFlight, err = envInt(envRetryBudgetMin, cfg.Retry.BudgetMinInFlight); err != nil {
		return err
	}
	if cfg.Retry.StreamTimeout, err = envDuration(envStreamTimeout, cfg.Retry.StreamTimeout); err != nil {
		return err
	}
	if cfg.Retry.StreamTimeout < 0 {
		return fmt.Errorf("config: %s must not be negative", envStreamTimeout)
	}
	if cfg.Retry.StreamIdleTimeout, err = envDuration(envStreamIdleTimeout, cfg.Retry.StreamIdleTimeout); err != nil {
		return err
	}
	if cfg.Retry.StreamIdleTimeout < 0 {
		return fmt.Errorf("config: %s must not be negative", envStreamIdleTimeout)
	}
	return nil
}

// loadQuota parses the reconcile protocol settings and the lease
// horizon; the horizon's lower bound needs the retry settings parsed
// just before it.
func loadQuota(cfg *Config) error {
	var err error
	if cfg.ReconcileInterval, err = envDuration(envReconcileInterval, cfg.ReconcileInterval); err != nil {
		return err
	}
	if cfg.ReconcileInterval < 0 {
		return fmt.Errorf("config: %s must not be negative", envReconcileInterval)
	}
	if cfg.Quota.LeaseTTL, err = envDuration(envQuotaLeaseTTL, cfg.Quota.LeaseTTL); err != nil {
		return err
	}
	if cfg.Quota.LeaseTTL < 0 {
		return fmt.Errorf("config: %s must not be negative", envQuotaLeaseTTL)
	}
	// A lease may only be reclaimed once the request holding it can no
	// longer spend tokens. That is only decidable here, where both the
	// request budget and the reclaim horizon are known: a horizon below
	// the budget silently refunds a request that really consumed tokens.
	if budget, bounded := cfg.requestBudget(); bounded && cfg.Quota.LeaseTTL > 0 && cfg.Quota.LeaseTTL <= budget {
		return fmt.Errorf("config: %s=%s must exceed the longest request the gateway will run (%s = %s + %s)",
			envQuotaLeaseTTL, cfg.Quota.LeaseTTL, budget, envRetryOverall, envStreamTimeout)
	}
	return nil
}

// loadCache parses the cache knobs; their bounds are enforced in
// validate only while the cache is enabled.
func loadCache(cfg *Config) error {
	var err error
	if cfg.Cache.Enabled, err = envBool(envCacheEnabled, cfg.Cache.Enabled); err != nil {
		return err
	}
	if cfg.Cache.TTL, err = envDuration(envCacheTTL, cfg.Cache.TTL); err != nil {
		return err
	}
	if cfg.Cache.Capacity, err = envInt(envCacheCapacity, cfg.Cache.Capacity); err != nil {
		return err
	}
	return nil
}

// loadCircuit parses the breaker knobs; their bounds are enforced in
// validate only while the breaker is enabled.
func loadCircuit(cfg *Config) error {
	var err error
	if cfg.Circuit.Enabled, err = envBool(envCircuitEnabled, cfg.Circuit.Enabled); err != nil {
		return err
	}
	cfg.Circuit.Strategy = envString(envCircuitStrategy, cfg.Circuit.Strategy)
	if cfg.Circuit.FailThreshold, err = envInt(envCircuitThreshold, cfg.Circuit.FailThreshold); err != nil {
		return err
	}
	if cfg.Circuit.Cooldown, err = envDuration(envCircuitCooldown, cfg.Circuit.Cooldown); err != nil {
		return err
	}
	if cfg.Circuit.ProbeTimeout, err = envDuration(envCircuitProbe, cfg.Circuit.ProbeTimeout); err != nil {
		return err
	}
	if cfg.Circuit.SlowRatio, err = envFloat(envCircuitSlowRatio, cfg.Circuit.SlowRatio); err != nil {
		return err
	}
	if cfg.Circuit.SlowCallThreshold, err = envDuration(envCircuitSlowThresh, cfg.Circuit.SlowCallThreshold); err != nil {
		return err
	}
	if cfg.Circuit.SlowCallThreshold < 0 {
		return fmt.Errorf("config: %s must not be negative", envCircuitSlowThresh)
	}
	return nil
}

// loadProbe parses the recovery-loop knobs; the timeout and threshold
// bounds apply only while the loop is enabled.
func loadProbe(cfg *Config) error {
	var err error
	if cfg.Probe.Interval, err = envDuration(envProbeInterval, cfg.Probe.Interval); err != nil {
		return err
	}
	if cfg.Probe.Timeout, err = envDuration(envProbeTimeout, cfg.Probe.Timeout); err != nil {
		return err
	}
	if cfg.Probe.Threshold, err = envInt(envProbeThreshold, cfg.Probe.Threshold); err != nil {
		return err
	}
	if cfg.Probe.BackoffMax, err = envDuration(envProbeBackoff, cfg.Probe.BackoffMax); err != nil {
		return err
	}
	return nil
}

// validate enforces the bounds one setting's parsed value must satisfy
// (some conditioned on its subsystem being enabled) and the shape of
// the model tables, after every setting is parsed.
func validate(cfg *Config) error {
	if cfg.Server.ShutdownGrace <= 0 {
		return fmt.Errorf("config: %s must be positive", envShutdownGrace)
	}
	switch cfg.Routing.Strategy {
	case "", "static", "latency":
	default:
		return fmt.Errorf("config: %s must be \"static\" or \"latency\"", envRouting)
	}
	if cfg.Obs.AccessLogQueueSize <= 0 {
		return fmt.Errorf("config: %s must be positive", envAccessLogQueue)
	}
	if cfg.Retry.MaxAttempts <= 0 {
		return fmt.Errorf("config: %s must be positive", envRetryMaxAttempts)
	}
	if cfg.Retry.BudgetMaxInFlight < 0 {
		return fmt.Errorf("config: %s must not be negative", envRetryBudget)
	}
	// Zero selects the fixed cap; anything else is a percentage of live
	// traffic, so a value above 100 would allow retries to outnumber
	// the requests serving them.
	if cfg.Retry.BudgetPercent < 0 || cfg.Retry.BudgetPercent > 100 {
		return fmt.Errorf("config: %s must be a percentage in [0, 100]", envRetryBudgetPercent)
	}
	if cfg.Retry.BudgetPercent > 0 && cfg.Retry.BudgetMinInFlight < 1 {
		return fmt.Errorf("config: %s must be at least 1 when %s is set", envRetryBudgetMin, envRetryBudgetPercent)
	}
	if cfg.Cache.Enabled {
		if cfg.Cache.TTL <= 0 {
			return fmt.Errorf("config: %s must be positive", envCacheTTL)
		}
		if cfg.Cache.Capacity <= 0 {
			return fmt.Errorf("config: %s must be positive", envCacheCapacity)
		}
	}
	if cfg.Circuit.Enabled {
		switch cfg.Circuit.Strategy {
		case "consecutive", "ratio":
		case "slow-call":
			if cfg.Circuit.SlowRatio <= 0 || cfg.Circuit.SlowRatio > 1 {
				return fmt.Errorf("config: %s must be a ratio in (0, 1]", envCircuitSlowRatio)
			}
			if cfg.Circuit.SlowCallThreshold <= 0 {
				return fmt.Errorf("config: %s must be positive when the strategy is slow-call: without it no attempt is ever slow", envCircuitSlowThresh)
			}
		default:
			return fmt.Errorf("config: %s must be \"consecutive\", \"ratio\" or \"slow-call\"", envCircuitStrategy)
		}
		if cfg.Circuit.FailThreshold <= 0 {
			return fmt.Errorf("config: %s must be positive", envCircuitThreshold)
		}
		if cfg.Circuit.Cooldown <= 0 {
			return fmt.Errorf("config: %s must be positive", envCircuitCooldown)
		}
		if cfg.Circuit.ProbeTimeout <= 0 {
			return fmt.Errorf("config: %s must be positive", envCircuitProbe)
		}
	}
	if cfg.Probe.Interval < 0 {
		return fmt.Errorf("config: %s must not be negative", envProbeInterval)
	}
	if cfg.Probe.BackoffMax < 0 {
		return fmt.Errorf("config: %s must not be negative", envProbeBackoff)
	}
	if cfg.Probe.BackoffMax > 0 && cfg.Probe.Interval <= 0 {
		return fmt.Errorf("config: %s needs %s to be set: there is no loop to back off within", envProbeBackoff, envProbeInterval)
	}
	if cfg.Probe.Interval > 0 && cfg.Probe.Timeout <= 0 {
		return fmt.Errorf("config: %s must be positive when %s is enabled", envProbeTimeout, envProbeInterval)
	}
	if cfg.Probe.Interval > 0 && cfg.Probe.Threshold < 1 {
		return fmt.Errorf("config: %s must be positive when %s is enabled", envProbeThreshold, envProbeInterval)
	}
	for model, limit := range cfg.ContextLimits {
		if model == "" {
			return fmt.Errorf("config: %s has an empty model name", envContextLimits)
		}
		if limit <= 0 {
			return fmt.Errorf("config: %s limits model %q must be positive", envContextLimits, model)
		}
	}
	for model, chain := range cfg.Fallbacks {
		if model == "" {
			return fmt.Errorf("config: %s has an empty model name", envFallbacks)
		}
		if len(chain) == 0 {
			return fmt.Errorf("config: %s lists no fallbacks for model %q", envFallbacks, model)
		}
		for _, f := range chain {
			if f == "" {
				return fmt.Errorf("config: %s has an empty fallback for model %q", envFallbacks, model)
			}
		}
	}
	return validateUpstreams(cfg)
}

// validateUpstreams refuses an upstream table that cannot serve
// reliably: an unusable id, a colliding identity, an unparsable or
// scheme-less base_url, an empty model list, a malformed model
// binding, or a dirty credential ring.
func validateUpstreams(cfg *Config) error {
	seenUpstreamIDs := make(map[string]struct{}, len(cfg.Upstreams))
	for i, u := range cfg.Upstreams {
		if u.ID == "" || u.BaseURL == "" {
			return fmt.Errorf("config: upstreams[%d] needs id and base_url", i)
		}
		// An id is a routing key, a metrics label, an access-log field
		// and an admin URL segment at once; a character outside the
		// documented set would only surface as a broken admin route or
		// an ambiguous key. Refuse at load, like every other identity
		// collision.
		if !validID(u.ID) {
			return fmt.Errorf("config: upstreams[%d] has an invalid id %q: ids are 1-128 characters of [A-Za-z0-9._-]", i, u.ID)
		}
		// A duplicated id would alias two distinct providers into one
		// circuit-breaker state and one admin target — the second copy
		// silently shadowing the first. Refuse at load, like every other
		// identity collision.
		if _, dup := seenUpstreamIDs[u.ID]; dup {
			return fmt.Errorf("config: upstreams[%d] repeats id %q", i, u.ID)
		}
		seenUpstreamIDs[u.ID] = struct{}{}
		// Scheme and host must parse now, not per request: a missing
		// scheme otherwise surfaces as a transport error against a
		// "healthy" gateway, 100% of requests failing with no startup
		// signal at all.
		parsed, err := url.Parse(u.BaseURL)
		if err != nil {
			return fmt.Errorf("config: upstreams[%d] (%s) has an unparsable base_url %q: %w", i, u.ID, u.BaseURL, err)
		}
		if (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
			return fmt.Errorf("config: upstreams[%d] (%s) base_url %q needs an http(s) scheme and a host", i, u.ID, u.BaseURL)
		}
		if len(u.Models) == 0 {
			return fmt.Errorf("config: upstreams[%d] (%s) lists no models", i, u.ID)
		}
		for _, m := range u.Models {
			if client, real, ok := strings.Cut(m, "="); ok && (client == "" || real == "" || client == "*") {
				return fmt.Errorf("config: upstreams[%d] (%s) has invalid model binding %q, want \"client=real\"", i, u.ID, m)
			}
		}
		// The credential ring indexes retirement and log trails by list
		// position, so the merged list must be clean: an empty entry
		// would send a broken Authorization header, and a duplicate
		// would rotate two slots onto one secret — a config smell that
		// must refuse to boot, not rotate uselessly.
		seenCredentials := make(map[string]struct{}, len(u.APIKeys)+1)
		for j, key := range u.Credentials() {
			if key == "" {
				return fmt.Errorf("config: upstreams[%d] (%s) has an empty credential at ring position %d", i, u.ID, j)
			}
			if _, dup := seenCredentials[key]; dup {
				return fmt.Errorf("config: upstreams[%d] (%s) repeats a credential", i, u.ID)
			}
			seenCredentials[key] = struct{}{}
		}
	}
	return nil
}

// loadIdentitySource resolves the static identity set: inline JSON, or a
// file:// URL naming a file whose content is the identity JSON. The file
// form keeps raw API keys off the process environment, where they are
// readable through /proc/<pid>/environ, `docker inspect` and CI logs.
// The path itself is operator configuration, not untrusted input.
func loadIdentitySource(key, raw string) (string, error) {
	if !strings.HasPrefix(raw, "file://") {
		return raw, nil
	}
	path := strings.TrimPrefix(raw, "file://")
	if path == "" {
		return "", fmt.Errorf("config: %s=%q names no file", key, raw)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("config: read %s: %w", key, err)
	}
	return string(data), nil
}

// guardDeploymentPosture refuses to boot a deployment that would serve
// without authentication: upstreams without any identity source turn the
// inference endpoints into an unauthenticated open proxy burning the
// configured upstream credentials, a PostgreSQL identity without an
// admin token leaves the key-minting management surface open, and any
// armed configuration without an admin token leaves the admin surface's
// live bindings (balance writes, breaker resets, model and upstream
// switches, the traffic record) to anonymous callers. All are legitimate
// local-development postures, so every refusal names its remedy and the
// explicit opt-in that overrides it.
func guardDeploymentPosture(cfg Config) error {
	insecure, err := envBool(envAllowUnauthenticated, false)
	if err != nil {
		return err
	}
	if insecure {
		return nil
	}
	if len(cfg.Upstreams) > 0 && cfg.Identity == "" && cfg.Postgres.DSN == "" {
		return fmt.Errorf("config: %s is set but no identity source is: the inference endpoints would serve every caller without authenticating them, burning the upstream credentials; set %s or %s, or set %s=1 to accept an unauthenticated deployment explicitly",
			envUpstreams, envIdentity, envPostgresDSN, envAllowUnauthenticated)
	}
	if cfg.Postgres.DSN != "" && cfg.Security.AdminToken == "" {
		return fmt.Errorf("config: %s is set but %s is empty: the identity management surface (user and api-key issuance) would be open to unauthenticated callers; set %s, or set %s=1 to accept an unauthenticated deployment explicitly",
			envPostgresDSN, envAdminToken, envAdminToken, envAllowUnauthenticated)
	}
	if cfg.Security.AdminToken == "" && adminSurfaceArmed(cfg) {
		return fmt.Errorf("config: %s is empty but the admin surface has live bindings (upstreams, identity or insights): balance writes, breaker resets and the model/upstream switches would be open to unauthenticated callers; set %s, or set %s=1 to accept an unauthenticated deployment explicitly",
			envAdminToken, envAdminToken, envAllowUnauthenticated)
	}
	return nil
}

// adminSurfaceArmed reports whether the configuration gives the admin
// surface anything live to guard. The surface mounts on every run, but a
// deployment with no upstreams, no identity and no insights store has
// nothing behind it but empty lists — the zero-configuration baseline
// stays bootable without a token.
func adminSurfaceArmed(cfg Config) bool {
	return len(cfg.Upstreams) > 0 || cfg.Identity != "" || cfg.Obs.InsightsDSN != ""
}

// validID reports whether a configured identifier (upstream ids today)
// stays inside the character set every consumer assumes: Redis key
// segments, metrics labels, access-log fields and admin URL path
// segments. A "/" or a space in an id would make the admin surface
// route it to the wrong endpoint and read ambiguously in a key or a
// label. The same rule governs static tenant ids (internal/auth).
var validID = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`).MatchString

func envString(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func envDuration(key string, fallback time.Duration) (time.Duration, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("config: parse %s=%q: %w", key, raw, err)
	}
	return d, nil
}

func envInt(key string, fallback int) (int, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("config: parse %s=%q: %w", key, raw, err)
	}
	return n, nil
}

// envFloat parses a floating-point environment variable with a
// fallback: ratio-shaped settings (the slow-call share) ride it.
func envFloat(key string, fallback float64) (float64, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}
	f, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, fmt.Errorf("config: parse %s=%q: %w", key, raw, err)
	}
	return f, nil
}

// envJSON decodes a JSON-valued environment variable into a slice; an
// unset or empty variable yields nil without error. Structured config
// (the upstream table) rides the same environment-only source as every
// other setting. The table's members are a fixed schema, so a typo'd
// member name (say "base_ur") fails the load instead of silently
// degrading the upstream it named; trailing data is rejected like any
// other malformed document.
func envJSON[T any](key string) ([]T, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return nil, nil
	}
	var out []T
	dec := json.NewDecoder(strings.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&out); err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", key, err)
	}
	if err := dec.Decode(&out); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("config: parse %s: trailing data after the JSON value", key)
	}
	return out, nil
}

// envJSONMap decodes a JSON-valued environment variable into a map; an
// unset or empty variable yields nil without error. There is no
// unknown-member check here on purpose: a map's keys ARE the data
// (model names), so every member is by definition known.
func envJSONMap[T any](key string) (map[string]T, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return nil, nil
	}
	var out map[string]T
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", key, err)
	}
	return out, nil
}

// envBool parses a boolean environment variable with a fallback.
func envBool(key string, fallback bool) (bool, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return fallback, nil
	}
	b, err := strconv.ParseBool(raw)
	if err != nil {
		return false, fmt.Errorf("config: parse %s=%q: %w", key, raw, err)
	}
	return b, nil
}
