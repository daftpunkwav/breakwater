/**
 * @file governance_test
 * @description Backend pairing for the governance state: the in-memory
 * mode, the Redis mode against a live miniredis, the startup refusal on
 * a dead backend, and the deployment namespace its keys are built from.
 */
package main

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/daftpunkwav/breakwater/internal/config"
)

func testConfig(addr string) config.Config {
	cfg, err := config.Load()
	if err != nil {
		panic(err)
	}
	cfg.Server.Addr = addr
	return cfg
}

func TestNewGovernanceMemoryMode(t *testing.T) {
	t.Parallel()
	gov, err := newGovernance(context.Background(), testConfig("127.0.0.1:0"))
	if err != nil {
		t.Fatalf("governance: %v", err)
	}
	defer gov.close()
	if gov.mode != "memory" || gov.limiter == nil || gov.ledger == nil || gov.sweepTarget == nil {
		t.Fatalf("memory governance incomplete: %+v", gov)
	}
	if gov.readiness != nil {
		t.Fatal("memory mode has no dependency to gate readiness on")
	}
}

func TestNewGovernanceRejectsDeadBackend(t *testing.T) {
	t.Parallel()
	cfg := testConfig("127.0.0.1:0")
	cfg.Redis.Addr = "127.0.0.1:1" // nothing listens here
	if _, err := newGovernance(context.Background(), cfg); err == nil {
		t.Fatal("a dead Redis at startup must fail assembly: the fail-closed limiter cannot serve")
	}
}

func TestNewGovernanceRedisMode(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	cfg := testConfig("127.0.0.1:0")
	cfg.Redis.Addr = mr.Addr()

	gov, err := newGovernance(context.Background(), cfg)
	if err != nil {
		t.Fatalf("governance: %v", err)
	}
	defer gov.close()

	if gov.mode != "redis" || gov.redisClient == nil || gov.snapshotSource == nil {
		t.Fatalf("redis governance incomplete: mode=%s", gov.mode)
	}
	if err := gov.readiness(); err != nil {
		t.Fatalf("readiness against a live miniredis: %v", err)
	}

	// A dead backend must fail the readiness probe: the fail-closed
	// limiter would reject everything, so the probe may not lie.
	mr.Close()
	if err := gov.readiness(); err == nil {
		t.Fatal("readiness must gate on the Redis backend")
	}
}

// TestRedisNamespaceIsTrimmedAndEmptySafe: an unset namespace keeps the
// bare key prefix, and a padded one is normalized, so the two spellings
// of the same deployment name cannot produce two key spaces.
func TestRedisNamespaceIsTrimmedAndEmptySafe(t *testing.T) {
	t.Parallel()
	if got := redisNamespace(config.Config{}); got != "" {
		t.Fatalf("namespace = %q, want empty", got)
	}
	cfg := config.Config{Redis: config.Redis{Namespace: "  staging  "}}
	if got := redisNamespace(cfg); got != "staging" {
		t.Fatalf("namespace = %q, want the trimmed name", got)
	}
}

// TestRedisOptionsPinClientTimeouts: the governance client's operation
// timeouts are pinned at assembly, not left on the library defaults —
// every request crosses this client serially, so a black-holed Redis
// must stall a request well under a second per stage, and the same
// bounds must hold on the TLS path.
func TestRedisOptionsPinClientTimeouts(t *testing.T) {
	t.Parallel()
	opts := redisOptions(config.Config{Redis: config.Redis{Addr: "127.0.0.1:6379"}})
	if opts.DialTimeout <= 0 || opts.ReadTimeout <= 0 || opts.WriteTimeout <= 0 {
		t.Fatalf("client timeouts unset: dial=%s read=%s write=%s, want explicit bounds",
			opts.DialTimeout, opts.ReadTimeout, opts.WriteTimeout)
	}
	if opts.TLSConfig != nil {
		t.Fatal("plaintext address must not grow a TLS config")
	}

	tlsOpts := redisOptions(config.Config{Redis: config.Redis{Addr: "redis.example.com:6380", TLS: true}})
	if tlsOpts.TLSConfig == nil || tlsOpts.TLSConfig.ServerName != "redis.example.com" {
		t.Fatalf("tls config = %+v, want one verifying against redis.example.com", tlsOpts.TLSConfig)
	}
	if tlsOpts.DialTimeout <= 0 || tlsOpts.ReadTimeout <= 0 || tlsOpts.WriteTimeout <= 0 {
		t.Fatalf("tls path client timeouts unset: dial=%s read=%s write=%s",
			tlsOpts.DialTimeout, tlsOpts.ReadTimeout, tlsOpts.WriteTimeout)
	}
}
