/**
 * @file load_test
 * @description The Load contract with a clean environment: the zero
 * configuration defaults and the per-variable override behavior.
 */
package config

import (
	"reflect"
	"testing"
	"time"
)

// envVars is every BREAKWATER_* variable Load reads; the fixtures
// neutralize all of them so the host environment cannot leak in.
var envVars = []string{
	envAddr, envShutdownGrace, envRedisAddr, envPostgresDSN,
	envAccessLogQueue, envUpstreams,
	envRetryMaxAttempts, envRetryAttemptTimeout, envRetryOverall,
	envRetryBackoffInitial, envRetryBackoffMax, envRetryBudget,
	envStreamTimeout, envReconcileInterval, envIdentity,
	envCacheEnabled, envCacheTTL, envCacheCapacity,
	envCircuitEnabled, envCircuitThreshold, envCircuitCooldown, envCircuitProbe,
	envAccessLogPath, envAdminToken,
}

// cleanEnv sets every BREAKWATER_* variable to the empty string, which
// Load treats as unset. Tests using it must stay sequential (t.Setenv
// forbids t.Parallel).
func cleanEnv(t *testing.T) {
	t.Helper()
	for _, key := range envVars {
		t.Setenv(key, "")
	}
}

// TestLoadDefaults: with zero configuration the gateway starts with
// the documented safe defaults, cache and breaker enabled.
func TestLoadDefaults(t *testing.T) {
	cleanEnv(t)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load with zero environment: %v", err)
	}

	want := Config{
		Server: Server{Addr: ":8080", ShutdownGrace: 15 * time.Second},
		Obs:    Obs{AccessLogQueueSize: 4096},
		Retry: Retry{
			MaxAttempts:       3,
			AttemptTimeout:    30 * time.Second,
			OverallDeadline:   60 * time.Second,
			BackoffInitial:    100 * time.Millisecond,
			BackoffMax:        2 * time.Second,
			BudgetMaxInFlight: 64,
			StreamTimeout:     10 * time.Minute,
		},
		ReconcileInterval: time.Minute,
		Cache: Cache{
			Enabled:  true,
			TTL:      60 * time.Second,
			Capacity: 1024,
		},
		Circuit: Circuit{
			Enabled:       true,
			FailThreshold: 5,
			Cooldown:      30 * time.Second,
			ProbeTimeout:  5 * time.Second,
		},
	}
	if !reflect.DeepEqual(cfg, want) {
		t.Fatalf("defaults =\n%+v\nwant\n%+v", cfg, want)
	}
	// Strings trimmed from an absent environment stay empty; the
	// structured upstream table is nil, not empty.
	if cfg.Postgres.DSN != "" || cfg.Identity != "" || cfg.Obs.AccessLogPath != "" ||
		cfg.Security.AdminToken != "" || cfg.Redis.Addr != "" {
		t.Fatalf("unset string variables must load empty, got %+v", cfg)
	}
	if cfg.Upstreams != nil {
		t.Fatalf("Upstreams = %+v, want nil without configuration", cfg.Upstreams)
	}
}

// TestLoadOverrides: every variable overrides its default, and string
// values are trimmed.
func TestLoadOverrides(t *testing.T) {
	cleanEnv(t)

	t.Setenv(envAddr, "127.0.0.1:9090")
	t.Setenv(envShutdownGrace, "45s")
	t.Setenv(envRedisAddr, "redis.internal:6379")
	t.Setenv(envPostgresDSN, "  postgres://db.local/bw  ")
	t.Setenv(envIdentity, "  {\"tiers\":[]}  ")
	t.Setenv(envAccessLogQueue, "8192")
	t.Setenv(envAccessLogPath, "  /var/log/bw.jsonl  ")
	t.Setenv(envAdminToken, "  admin-secret  ")
	t.Setenv(envUpstreams, `[{"id":"u1","base_url":"http://127.0.0.1:8090","api_key":"k","probe_url":"/healthz","models":["m1","*"]}]`)
	t.Setenv(envRetryMaxAttempts, "5")
	t.Setenv(envRetryAttemptTimeout, "10s")
	t.Setenv(envRetryOverall, "90s")
	t.Setenv(envRetryBackoffInitial, "50ms")
	t.Setenv(envRetryBackoffMax, "4s")
	t.Setenv(envRetryBudget, "128")
	t.Setenv(envStreamTimeout, "0s")
	t.Setenv(envReconcileInterval, "2m")
	t.Setenv(envCacheEnabled, "false")
	t.Setenv(envCacheTTL, "30s")
	t.Setenv(envCacheCapacity, "2048")
	t.Setenv(envCircuitEnabled, "false")
	t.Setenv(envCircuitThreshold, "9")
	t.Setenv(envCircuitCooldown, "45s")
	t.Setenv(envCircuitProbe, "3s")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load with overrides: %v", err)
	}

	if cfg.Server.Addr != "127.0.0.1:9090" || cfg.Server.ShutdownGrace != 45*time.Second {
		t.Fatalf("server = %+v", cfg.Server)
	}
	if cfg.Redis.Addr != "redis.internal:6379" {
		t.Fatalf("redis = %+v", cfg.Redis)
	}
	// Free-form string values are trimmed; structured JSON is not.
	if cfg.Postgres.DSN != "postgres://db.local/bw" || cfg.Identity != `{"tiers":[]}` ||
		cfg.Obs.AccessLogPath != "/var/log/bw.jsonl" || cfg.Security.AdminToken != "admin-secret" {
		t.Fatalf("trimmed strings = dsn %q identity %q path %q token %q",
			cfg.Postgres.DSN, cfg.Identity, cfg.Obs.AccessLogPath, cfg.Security.AdminToken)
	}
	if cfg.Obs.AccessLogQueueSize != 8192 {
		t.Fatalf("queue size = %d", cfg.Obs.AccessLogQueueSize)
	}
	if len(cfg.Upstreams) != 1 {
		t.Fatalf("Upstreams = %+v, want one parsed entry", cfg.Upstreams)
	}
	up := cfg.Upstreams[0]
	if up.ID != "u1" || up.BaseURL != "http://127.0.0.1:8090" || up.APIKey != "k" ||
		up.ProbeURL != "/healthz" || len(up.Models) != 2 || up.Models[1] != "*" {
		t.Fatalf("upstream = %+v", up)
	}
	if cfg.Retry != (Retry{
		MaxAttempts:       5,
		AttemptTimeout:    10 * time.Second,
		OverallDeadline:   90 * time.Second,
		BackoffInitial:    50 * time.Millisecond,
		BackoffMax:        4 * time.Second,
		BudgetMaxInFlight: 128,
		StreamTimeout:     0,
	}) {
		t.Fatalf("retry = %+v", cfg.Retry)
	}
	if cfg.ReconcileInterval != 2*time.Minute {
		t.Fatalf("reconcile interval = %s", cfg.ReconcileInterval)
	}
	if cfg.Cache != (Cache{Enabled: false, TTL: 30 * time.Second, Capacity: 2048}) {
		t.Fatalf("cache = %+v", cfg.Cache)
	}
	if cfg.Circuit != (Circuit{Enabled: false, FailThreshold: 9, Cooldown: 45 * time.Second, ProbeTimeout: 3 * time.Second}) {
		t.Fatalf("circuit = %+v", cfg.Circuit)
	}
}

// TestLoadDisabledSubsystemsSkipSizing: with the cache or breaker
// disabled, non-positive sizing values load instead of failing.
func TestLoadDisabledSubsystemsSkipSizing(t *testing.T) {
	cleanEnv(t)
	t.Setenv(envCacheEnabled, "false")
	t.Setenv(envCacheTTL, "0s")
	t.Setenv(envCacheCapacity, "0")
	t.Setenv(envCircuitEnabled, "false")
	t.Setenv(envCircuitThreshold, "0")
	t.Setenv(envCircuitCooldown, "0s")
	t.Setenv(envCircuitProbe, "0s")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load with disabled subsystems: %v", err)
	}
	if cfg.Cache.Enabled || cfg.Circuit.Enabled {
		t.Fatalf("subsystems = cache %v circuit %v, want both disabled", cfg.Cache, cfg.Circuit)
	}
}
