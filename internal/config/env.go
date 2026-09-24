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
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Defaults keep the gateway runnable with zero configuration. They are
// provisional tuning values and may change during implementation.
const (
	defaultAddr              = ":8080"
	defaultShutdownGrace     = 15 * time.Second
	defaultRedisAddr         = "127.0.0.1:6379"
	defaultAccessLogQueue    = 4096
	defaultMaxAttempts       = 3
	defaultAttemptTimeout    = 30 * time.Second
	defaultOverallDeadline   = 60 * time.Second
	defaultBackoffInitial    = 100 * time.Millisecond
	defaultBackoffMax        = 2 * time.Second
	defaultRetryBudgetRadius = 64
	defaultCacheTTL          = 60 * time.Second
	defaultCacheCapacity     = 1024
	defaultCircuitThreshold  = 5
	defaultCircuitCooldown   = 30 * time.Second
	defaultCircuitProbe      = 5 * time.Second
)

// Environment variable names.
const (
	envAddr           = "BREAKWATER_ADDR"
	envShutdownGrace  = "BREAKWATER_SHUTDOWN_GRACE"
	envRedisAddr      = "BREAKWATER_REDIS_ADDR"
	envPostgresDSN    = "BREAKWATER_POSTGRES_DSN"
	envAccessLogQueue = "BREAKWATER_ACCESS_LOG_QUEUE_SIZE"
	envUpstreams      = "BREAKWATER_UPSTREAMS"

	envRetryMaxAttempts    = "BREAKWATER_RETRY_MAX_ATTEMPTS"
	envRetryAttemptTimeout = "BREAKWATER_RETRY_ATTEMPT_TIMEOUT"
	envRetryOverall        = "BREAKWATER_RETRY_OVERALL_DEADLINE"
	envRetryBackoffInitial = "BREAKWATER_RETRY_BACKOFF_INITIAL"
	envRetryBackoffMax     = "BREAKWATER_RETRY_BACKOFF_MAX"
	envRetryBudget         = "BREAKWATER_RETRY_BUDGET_MAX_IN_FLIGHT"
	envIdentity            = "BREAKWATER_IDENTITY"

	envCacheEnabled  = "BREAKWATER_CACHE_ENABLED"
	envCacheTTL      = "BREAKWATER_CACHE_TTL"
	envCacheCapacity = "BREAKWATER_CACHE_CAPACITY"

	envCircuitEnabled   = "BREAKWATER_CIRCUIT_ENABLED"
	envCircuitThreshold = "BREAKWATER_CIRCUIT_FAIL_THRESHOLD"
	envCircuitCooldown  = "BREAKWATER_CIRCUIT_COOLDOWN"
	envCircuitProbe     = "BREAKWATER_CIRCUIT_PROBE_TIMEOUT"
)

// Load reads the configuration from the environment and validates it.
func Load() (Config, error) {
	cfg := Config{
		Server: Server{
			Addr:          envString(envAddr, defaultAddr),
			ShutdownGrace: defaultShutdownGrace,
		},
		Redis: Redis{
			Addr: envString(envRedisAddr, defaultRedisAddr),
		},
		Postgres: Postgres{
			DSN: strings.TrimSpace(os.Getenv(envPostgresDSN)),
		},
		Identity: strings.TrimSpace(os.Getenv(envIdentity)),
		Obs: Obs{
			AccessLogQueueSize: defaultAccessLogQueue,
		},
		Retry: Retry{
			MaxAttempts:       defaultMaxAttempts,
			AttemptTimeout:    defaultAttemptTimeout,
			OverallDeadline:   defaultOverallDeadline,
			BackoffInitial:    defaultBackoffInitial,
			BackoffMax:        defaultBackoffMax,
			BudgetMaxInFlight: defaultRetryBudgetRadius,
		},
		Cache: Cache{
			Enabled:  true,
			TTL:      defaultCacheTTL,
			Capacity: defaultCacheCapacity,
		},
		Circuit: Circuit{
			Enabled:       true,
			FailThreshold: defaultCircuitThreshold,
			Cooldown:      defaultCircuitCooldown,
			ProbeTimeout:  defaultCircuitProbe,
		},
	}

	var err error
	if cfg.Server.ShutdownGrace, err = envDuration(envShutdownGrace, cfg.Server.ShutdownGrace); err != nil {
		return Config{}, err
	}
	if cfg.Obs.AccessLogQueueSize, err = envInt(envAccessLogQueue, cfg.Obs.AccessLogQueueSize); err != nil {
		return Config{}, err
	}
	if cfg.Upstreams, err = envJSON[Upstream](envUpstreams); err != nil {
		return Config{}, err
	}
	if cfg.Retry.MaxAttempts, err = envInt(envRetryMaxAttempts, cfg.Retry.MaxAttempts); err != nil {
		return Config{}, err
	}
	if cfg.Retry.AttemptTimeout, err = envDuration(envRetryAttemptTimeout, cfg.Retry.AttemptTimeout); err != nil {
		return Config{}, err
	}
	if cfg.Retry.OverallDeadline, err = envDuration(envRetryOverall, cfg.Retry.OverallDeadline); err != nil {
		return Config{}, err
	}
	if cfg.Retry.BackoffInitial, err = envDuration(envRetryBackoffInitial, cfg.Retry.BackoffInitial); err != nil {
		return Config{}, err
	}
	if cfg.Retry.BackoffMax, err = envDuration(envRetryBackoffMax, cfg.Retry.BackoffMax); err != nil {
		return Config{}, err
	}
	if cfg.Retry.BudgetMaxInFlight, err = envInt(envRetryBudget, cfg.Retry.BudgetMaxInFlight); err != nil {
		return Config{}, err
	}
	if cfg.Cache.Enabled, err = envBool(envCacheEnabled, cfg.Cache.Enabled); err != nil {
		return Config{}, err
	}
	if cfg.Cache.TTL, err = envDuration(envCacheTTL, cfg.Cache.TTL); err != nil {
		return Config{}, err
	}
	if cfg.Cache.Capacity, err = envInt(envCacheCapacity, cfg.Cache.Capacity); err != nil {
		return Config{}, err
	}
	if cfg.Circuit.Enabled, err = envBool(envCircuitEnabled, cfg.Circuit.Enabled); err != nil {
		return Config{}, err
	}
	if cfg.Circuit.FailThreshold, err = envInt(envCircuitThreshold, cfg.Circuit.FailThreshold); err != nil {
		return Config{}, err
	}
	if cfg.Circuit.Cooldown, err = envDuration(envCircuitCooldown, cfg.Circuit.Cooldown); err != nil {
		return Config{}, err
	}
	if cfg.Circuit.ProbeTimeout, err = envDuration(envCircuitProbe, cfg.Circuit.ProbeTimeout); err != nil {
		return Config{}, err
	}

	if cfg.Server.ShutdownGrace <= 0 {
		return Config{}, fmt.Errorf("config: %s must be positive", envShutdownGrace)
	}
	if cfg.Obs.AccessLogQueueSize <= 0 {
		return Config{}, fmt.Errorf("config: %s must be positive", envAccessLogQueue)
	}
	if cfg.Retry.MaxAttempts <= 0 {
		return Config{}, fmt.Errorf("config: %s must be positive", envRetryMaxAttempts)
	}
	if cfg.Retry.BudgetMaxInFlight < 0 {
		return Config{}, fmt.Errorf("config: %s must not be negative", envRetryBudget)
	}
	if cfg.Cache.Enabled {
		if cfg.Cache.TTL <= 0 {
			return Config{}, fmt.Errorf("config: %s must be positive", envCacheTTL)
		}
		if cfg.Cache.Capacity <= 0 {
			return Config{}, fmt.Errorf("config: %s must be positive", envCacheCapacity)
		}
	}
	if cfg.Circuit.Enabled {
		if cfg.Circuit.FailThreshold <= 0 {
			return Config{}, fmt.Errorf("config: %s must be positive", envCircuitThreshold)
		}
		if cfg.Circuit.Cooldown <= 0 {
			return Config{}, fmt.Errorf("config: %s must be positive", envCircuitCooldown)
		}
		if cfg.Circuit.ProbeTimeout <= 0 {
			return Config{}, fmt.Errorf("config: %s must be positive", envCircuitProbe)
		}
	}
	for i, u := range cfg.Upstreams {
		if u.ID == "" || u.BaseURL == "" {
			return Config{}, fmt.Errorf("config: upstreams[%d] needs id and base_url", i)
		}
		if len(u.Models) == 0 {
			return Config{}, fmt.Errorf("config: upstreams[%d] (%s) lists no models", i, u.ID)
		}
	}
	return cfg, nil
}

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

// envJSON decodes a JSON-valued environment variable into a slice; an
// unset or empty variable yields nil without error. Structured config
// (the upstream table) rides the same environment-only source as every
// other setting.
func envJSON[T any](key string) ([]T, error) {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return nil, nil
	}
	var out []T
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
