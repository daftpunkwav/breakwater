/**
 * @file redis
 * @description The Redis-backed quota ledger: balance, leases and the
 * sweep index, every transition executed atomically by Lua.
 *
 * Responsibilities:
 * - Guarantee no over-draft under concurrency: the balance check,
 *   deduction and lease record are one script invocation
 * - Guarantee every lease converges: RESERVED leases age out of the
 *   sweep zset into a terminal state exactly once
 * - Nothing else: sweep scheduling belongs to the sweeper, degradation
 *   policy to the pipeline layer
 */
package quota

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

// reserveScriptSrc atomically deducts the balance and records the lease.
//
//go:embed reserve.lua
var reserveScriptSrc string

// settleScriptSrc reconciles a live lease against actual usage.
//
//go:embed settle.lua
var settleScriptSrc string

// releaseScriptSrc moves a RESERVED lease to a terminal state and
// refunds its full amount exactly once.
//
//go:embed release.lua
var releaseScriptSrc string

// keyPrefix namespaces every quota key.
const keyPrefix = "bw:quota:"

// Redis is the Redis-backed Ledger. It is safe for concurrent use.
type Redis struct {
	rdb           *redis.Client
	reserveScript *redis.Script
	settleScript  *redis.Script
	releaseScript *redis.Script
}

// NewRedis builds the ledger over a ready client.
func NewRedis(client *redis.Client) *Redis {
	return &Redis{
		rdb:           client,
		reserveScript: redis.NewScript(reserveScriptSrc),
		settleScript:  redis.NewScript(settleScriptSrc),
		releaseScript: redis.NewScript(releaseScriptSrc),
	}
}

// Ping reports backend health for readiness probes.
func (r *Redis) Ping(ctx context.Context) error {
	return r.rdb.Ping(ctx).Err()
}

func balanceKey(tenantID string) string { return keyPrefix + "bal:" + tenantID }
func leaseKey(leaseID string) string    { return keyPrefix + "lease:" + leaseID }

// sweepKey is the process-shared zset of live leases scored by their
// expiry instant in milliseconds.
func sweepKey() string { return keyPrefix + "sweep" }

// SetBalance provisions a tenant balance (admin and dev surface).
func (r *Redis) SetBalance(ctx context.Context, tenantID string, balance int64) error {
	return r.rdb.Set(ctx, balanceKey(tenantID), balance, 0).Err()
}

// EnsureBalance provisions the balance only when the tenant has none,
// so restarts never silently reset accounting; it reports whether it
// created the balance.
func (r *Redis) EnsureBalance(ctx context.Context, tenantID string, initial int64) (bool, error) {
	created, err := r.rdb.SetNX(ctx, balanceKey(tenantID), initial, 0).Result()
	if err != nil {
		return false, fmt.Errorf("quota: ensure balance: %w", err)
	}
	return created, nil
}

// Reserve implements Ledger.
func (r *Redis) Reserve(ctx context.Context, tenantID string, amount int64) (Lease, error) {
	now := time.Now()
	lease := Lease{
		ID:        newLeaseID(),
		TenantID:  tenantID,
		Amount:    amount,
		State:     LeaseStateReserved,
		CreatedAt: now,
	}
	res, err := r.reserveScript.Run(ctx, r.rdb,
		[]string{balanceKey(tenantID), leaseKey(lease.ID), sweepKey()},
		now.UnixMilli(), amount, defaultLeaseTTL.Milliseconds(), lease.ID, tenantID,
	).Slice()
	if err != nil {
		return Lease{}, fmt.Errorf("quota: reserve script: %w", err)
	}
	ok, _ := res[0].(int64)
	if ok != 1 {
		return Lease{}, ErrInsufficientBalance
	}
	return lease, nil
}

// Settle implements Ledger. The tenant is read back from the lease
// record first: settlement runs after the response, so the extra
// round-trip stays off the hot path.
func (r *Redis) Settle(ctx context.Context, leaseID string, usedTokens int64) error {
	tenant, err := r.rdb.HGet(ctx, leaseKey(leaseID), "tenant").Result()
	if errors.Is(err, redis.Nil) {
		// Unknown lease: settling it would be a no-op in the script too.
		return nil
	}
	if err != nil {
		// A backend failure must surface, never masquerade as an
		// unknown lease: the middleware logs it and the sweeper still
		// converges the lease.
		return fmt.Errorf("quota: settle lease lookup: %w", err)
	}
	res, err := r.settleScript.Run(ctx, r.rdb,
		[]string{balanceKey(tenant), leaseKey(leaseID), sweepKey()},
		usedTokens, leaseID, leaseAuditTTL.Milliseconds(),
	).Slice()
	if err != nil {
		return fmt.Errorf("quota: settle script: %w", err)
	}
	if moved, _ := res[0].(int64); moved == 0 {
		// Detectable no-op: the lease was already terminal. Late settles
		// after a sweeper expiry are expected, not errors; the contract
		// requires surfacing them.
		slog.Info("quota settle reached a terminal lease", "lease", leaseID, "state", res[1])
	}
	return nil
}

// Cancel implements Ledger.
func (r *Redis) Cancel(ctx context.Context, leaseID string) error {
	moved, err := r.terminate(ctx, leaseID, LeaseStateCancelled)
	if err != nil {
		return err
	}
	if !moved {
		// Detectable no-op, surfaced per the Ledger contract: the lease
		// was already terminal or its record is gone.
		slog.Info("quota cancel reached a missing or terminal lease", "lease", leaseID)
	}
	return nil
}

// terminate moves a RESERVED lease to a terminal state through the
// release script; moved reports whether this call performed the
// transition (false for unknown or already-terminal leases).
func (r *Redis) terminate(ctx context.Context, leaseID string, state LeaseState) (bool, error) {
	tenant, err := r.rdb.HGet(ctx, leaseKey(leaseID), "tenant").Result()
	if errors.Is(err, redis.Nil) {
		// The record is gone: terminal past its audit window, or never
		// provisioned. Drop the stale sweep entry so the zset cannot
		// accumulate ghosts the sweeper would fetch forever.
		if err := r.rdb.ZRem(ctx, sweepKey(), leaseID).Err(); err != nil {
			return false, fmt.Errorf("quota: sweep entry cleanup: %w", err)
		}
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("quota: lease lookup: %w", err)
	}
	res, err := r.releaseScript.Run(ctx, r.rdb,
		[]string{balanceKey(tenant), leaseKey(leaseID), sweepKey()},
		leaseID, string(state), leaseAuditTTL.Milliseconds(),
	).Int64()
	if err != nil {
		return false, fmt.Errorf("quota: release script: %w", err)
	}
	return res == 1, nil
}

// Balance implements Ledger.
func (r *Redis) Balance(ctx context.Context, tenantID string) (int64, error) {
	bal, err := r.rdb.Get(ctx, balanceKey(tenantID)).Int64()
	if errors.Is(err, redis.Nil) {
		// Unprovisioned tenants are reported, not read as zero: the
		// admin API must tell "no ledger" apart from "drained".
		return 0, ErrUnknownTenant
	}
	if err != nil {
		return 0, fmt.Errorf("quota: balance: %w", err)
	}
	return bal, nil
}

// defaultLeaseTTL bounds how long a RESERVED lease may live before the
// sweeper reclaims it. Long streams must settle within it.
const defaultLeaseTTL = 10 * time.Minute

// leaseAuditTTL bounds how long a terminal lease record is retained for
// audit before it expires; without it the hash set would grow without
// bound. A RESERVED lease never carries this TTL — its refund depends
// on the record surviving until the sweeper sees it.
const leaseAuditTTL = time.Hour

// SweepOnce reclaims expired RESERVED leases through the release
// script, which moves each one to EXPIRED and refunds its amount
// exactly once. It returns how many leases were reclaimed.
func (r *Redis) SweepOnce(ctx context.Context, now time.Time, limit int) (int, error) {
	ids, err := r.rdb.ZRangeByScore(ctx, sweepKey(), &redis.ZRangeBy{
		Min:   "-inf",
		Max:   fmt.Sprintf("%d", now.UnixMilli()),
		Count: int64(limit),
	}).Result()
	if err != nil {
		return 0, fmt.Errorf("quota: sweep scan: %w", err)
	}
	expired := 0
	for _, id := range ids {
		moved, err := r.terminate(ctx, id, LeaseStateExpired)
		if err != nil {
			return expired, err
		}
		if moved {
			expired++
		}
	}
	return expired, nil
}
