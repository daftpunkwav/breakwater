/**
 * @file governance
 * @description Assembly helpers of the gateway binary: which backend
 * serves each governance concern, and how identity resolves.
 *
 * Responsibilities:
 * - Choose the limiter and ledger backends: Redis when configured, the
 *   in-memory pair otherwise (development and evidence runs)
 * - Choose the identity source: PostgreSQL system of record when a DSN
 *   is set, the static identity set otherwise; both wrapped in the
 *   process-local LRU
 * - Nothing else: behavior lives in the packages, decisions live here
 */
package main

import (
	"context"
	"crypto/tls"
	"log/slog"
	"net"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/daftpunkwav/breakwater/internal/auth"
	"github.com/daftpunkwav/breakwater/internal/config"
	"github.com/daftpunkwav/breakwater/internal/limiter"
	"github.com/daftpunkwav/breakwater/internal/quota"
)

// governance bundles the concrete governance backends one process runs
// with.
type governance struct {
	// mode names the backend pairing for startup logs.
	mode        string
	limiter     limiter.Limiter
	ledger      quota.Ledger
	sweepTarget quota.SweepTarget
	// snapshotSource exposes the Redis ledger's reconcile readings; nil
	// in memory mode.
	snapshotSource quota.SnapshotSource
	redisClient    *redis.Client
	// readiness gates business traffic on the backend the fail-closed
	// limiter depends on; nil in memory mode (always ready).
	readiness func() error
	close     func()
}

// balanceSeeder is the provisioning surface both ledger backends share.
type balanceSeeder interface {
	EnsureBalance(ctx context.Context, tenantID string, initial int64) (bool, error)
}

// newGovernance builds the backend pairing for the configuration.
// A configured Redis must answer at startup: with a fail-closed limiter
// a dead backend is a deployment failure, not a degraded start.
func newGovernance(ctx context.Context, cfg config.Config) (*governance, error) {
	if cfg.Redis.Addr == "" {
		mem := quota.NewMemory().WithLeaseTTL(cfg.LeaseTTL())
		return &governance{
			mode:        "memory",
			limiter:     limiter.NewMemory(),
			ledger:      mem,
			sweepTarget: mem,
			readiness:   nil,
			close:       func() {},
		}, nil
	}

	rdb := redis.NewClient(redisOptions(cfg))
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		_ = rdb.Close()
		return nil, err
	}

	redisLedger := quota.NewRedis(rdb, redisNamespace(cfg), cfg.LeaseTTL())
	return &governance{
		mode:           "redis",
		limiter:        limiter.NewRedis(rdb, redisNamespace(cfg)),
		ledger:         redisLedger,
		sweepTarget:    redisLedger,
		snapshotSource: redisLedger,
		redisClient:    rdb,
		readiness: func() error {
			pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			return rdb.Ping(pingCtx).Err()
		},
		close: func() { _ = rdb.Close() },
	}, nil
}

// redisNamespace is the deployment name this gateway's hot state lives
// under. Balances, rate limit buckets and lease records all sit inside
// it, so an empty namespace is only safe when this gateway owns its
// Redis instance outright.
func redisNamespace(cfg config.Config) string {
	return strings.TrimSpace(cfg.Redis.Namespace)
}

// redisOptions builds the client options for the configured Redis:
// plaintext by default, TLS when the deployment opts in. The server
// name comes from the address host so certificates verify against it
// even when the address carries a port.
func redisOptions(cfg config.Config) *redis.Options {
	opts := &redis.Options{Addr: cfg.Redis.Addr}
	if !cfg.Redis.TLS {
		return opts
	}
	host, _, err := net.SplitHostPort(cfg.Redis.Addr)
	if err != nil {
		host = cfg.Redis.Addr
	}
	opts.TLSConfig = &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12}
	return opts
}

// newAuthStore resolves the identity source. A nil store means no
// identity is configured and the gateway runs without governance
// stages. The readiness probe covers the dependency authentication
// fail-closes on (the identity database); nil for the static set.
// The administration port rides along: PostgreSQL is the one source
// with a management surface; the static set is configuration, not a
// surface.
func newAuthStore(ctx context.Context, cfg config.Config) (auth.Store, *auth.Static, auth.AdminStore, func(), func() error, error) {
	switch {
	case cfg.Postgres.DSN != "":
		pg, err := auth.NewPG(ctx, cfg.Postgres.DSN)
		if err != nil {
			return nil, nil, nil, nil, nil, err
		}
		probe := func() error {
			pingCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			return pg.Ping(pingCtx)
		}
		return auth.NewCachedStore(pg, authPosTTL, authNegTTL), nil, pg, pg.Close, probe, nil

	case cfg.Identity != "":
		staticCfg, err := auth.ParseStaticConfig([]byte(cfg.Identity))
		if err != nil {
			return nil, nil, nil, nil, nil, err
		}
		staticStore, err := auth.NewStatic(staticCfg)
		if err != nil {
			return nil, nil, nil, nil, nil, err
		}
		return auth.NewCachedStore(staticStore, authPosTTL, authNegTTL), staticStore, nil, func() {}, nil, nil

	default:
		return nil, nil, nil, func() {}, nil, nil
	}
}

// seedBalances provisions each static-identity tenant with its tier's
// monthly budget, only when no balance exists yet — restarts never
// reset accounting (evidence runs delete the Redis state explicitly).
func seedBalances(ctx context.Context, identity *auth.Static, ledger quota.Ledger, logger *slog.Logger) {
	seeder, ok := ledger.(balanceSeeder)
	if !ok {
		return
	}
	for _, tenantID := range identity.Tenants() {
		tenant, ok := identity.TenantByID(tenantID)
		if !ok {
			continue
		}
		// A tier may legitimately budget nothing. Provisioning a zero
		// balance is what keeps that a spending decision (402, the
		// budget is gone) rather than a provisioning fault (503, the
		// tenant has no ledger) — the two are different problems and
		// the operator needs to be told which one they have.
		created, err := seeder.EnsureBalance(ctx, tenantID, tenant.Tier.MonthlyQuota)
		if err != nil {
			logger.Warn("seed tenant balance", "tenant", tenantID, "error", err)
			continue
		}
		if created {
			logger.Info("seeded tenant balance", "tenant", tenantID, "tokens", tenant.Tier.MonthlyQuota)
		}
	}
}
