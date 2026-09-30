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

// neverProvisioned is the balance value reserve.lua returns for a
// tenant with no ledger entry, distinct from any real balance.
const neverProvisioned = -1

// defaultKeyPrefix namespaces every quota key when the deployment has
// no namespace of its own. Safe only when this gateway owns its Redis
// instance: balances and lease records are money and must not be shared
// with another environment by accident.
const defaultKeyPrefix = "bw:quota:"

// Redis is the Redis-backed Ledger. It is safe for concurrent use.
type Redis struct {
	rdb           *redis.Client
	reserveScript *redis.Script
	settleScript  *redis.Script
	releaseScript *redis.Script
	keyPrefix     string
	leaseTTL      time.Duration
}

// NewRedis builds the ledger over a ready client. namespace scopes this
// deployment's keys inside a shared Redis instance; empty means this
// gateway owns the instance outright. leaseTTL is how long a RESERVED
// lease may live before the sweeper reclaims it; it must outlast the
// longest request the gateway will run, or a live request's reservation
// is refunded while it is still spending tokens.
func NewRedis(client *redis.Client, namespace string, leaseTTL time.Duration) *Redis {
	return &Redis{
		rdb:           client,
		reserveScript: redis.NewScript(reserveScriptSrc),
		settleScript:  redis.NewScript(settleScriptSrc),
		releaseScript: redis.NewScript(releaseScriptSrc),
		keyPrefix:     namespacedKeyPrefix(namespace),
		leaseTTL:      leaseTTL,
	}
}

// namespacedKeyPrefix places a deployment namespace in front of the
// package's own key base, so two environments sharing one Redis instance
// never read each other's balances.
func namespacedKeyPrefix(namespace string) string {
	if namespace == "" {
		return defaultKeyPrefix
	}
	return namespace + ":" + defaultKeyPrefix
}

// Ping reports backend health for readiness probes.
func (r *Redis) Ping(ctx context.Context) error {
	return r.rdb.Ping(ctx).Err()
}

func (r *Redis) balanceKey(tenantID string) string { return r.keyPrefix + "bal:" + tenantID }
func (r *Redis) leaseKey(leaseID string) string    { return r.keyPrefix + "lease:" + leaseID }

// consumedKey holds the lifetime actual-usage total (observation input;
// not part of the reconcile identity), refundedKey the lifetime refunds
// and debitedKey the lifetime reservation debits — the last two pair
// with every balance movement and drive the reconcile identity.
func (r *Redis) consumedKey(tenantID string) string { return r.keyPrefix + "consumed:" + tenantID }
func (r *Redis) refundedKey(tenantID string) string { return r.keyPrefix + "refunded:" + tenantID }
func (r *Redis) debitedKey(tenantID string) string  { return r.keyPrefix + "debited:" + tenantID }

// epochKey counts balance corrections (admin SetBalance): the
// reconciler skips the interval across an epoch bump, since a manual
// balance change is not consumable drift.
func (r *Redis) epochKey(tenantID string) string { return r.keyPrefix + "epoch:" + tenantID }

// sweepKey is the process-shared zset of live leases scored by their
// expiry instant in milliseconds.
func (r *Redis) sweepKey() string { return r.keyPrefix + "sweep" }

// SetBalance implements Ledger. It also bumps the reconcile epoch so
// the reconciler skips the interval across a manual correction — a
// top-up is not consumable drift.
func (r *Redis) SetBalance(ctx context.Context, tenantID string, balance int64) error {
	pipe := r.rdb.TxPipeline()
	pipe.Set(ctx, r.balanceKey(tenantID), balance, 0)
	pipe.Incr(ctx, r.epochKey(tenantID))
	_, err := pipe.Exec(ctx)
	return err
}

// EnsureBalance provisions the balance only when the tenant has none,
// so restarts never silently reset accounting; it reports whether it
// created the balance.
func (r *Redis) EnsureBalance(ctx context.Context, tenantID string, initial int64) (bool, error) {
	created, err := r.rdb.SetNX(ctx, r.balanceKey(tenantID), initial, 0).Result()
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
		[]string{r.balanceKey(tenantID), r.leaseKey(lease.ID), r.sweepKey(), r.debitedKey(tenantID)},
		now.UnixMilli(), amount, r.leaseTTL.Milliseconds(), lease.ID, tenantID,
	).Slice()
	if err != nil {
		return Lease{}, fmt.Errorf("quota: reserve script: %w", err)
	}
	ok, _ := res[0].(int64)
	if ok != 1 {
		// The script separates "never provisioned" (a -1 balance) from
		// "balance too low" (the balance itself). Reporting the second as
		// the first would tell a client it is out of money when the real
		// fault is a missing provisioning record.
		if bal, _ := res[1].(int64); bal == neverProvisioned {
			return Lease{}, ErrUnknownTenant
		}
		return Lease{}, ErrInsufficientBalance
	}
	return lease, nil
}

// Settle implements Ledger. The tenant is read back from the lease
// record first: settlement runs after the response, so the extra
// round-trip stays off the hot path.
func (r *Redis) Settle(ctx context.Context, leaseID string, usedTokens int64) error {
	tenant, err := r.rdb.HGet(ctx, r.leaseKey(leaseID), "tenant").Result()
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
		[]string{r.balanceKey(tenant), r.leaseKey(leaseID), r.sweepKey(), r.consumedKey(tenant), r.refundedKey(tenant)},
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
	moved, err := r.release(ctx, leaseID, LeaseStateCancelled)
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

// release moves a RESERVED lease to a terminal state through the
// release script; moved reports whether this call performed the
// transition (false for unknown or already-terminal leases).
func (r *Redis) release(ctx context.Context, leaseID string, state LeaseState) (bool, error) {
	tenant, err := r.rdb.HGet(ctx, r.leaseKey(leaseID), "tenant").Result()
	if errors.Is(err, redis.Nil) {
		// The record is gone: terminal past its audit window, or never
		// provisioned. Drop the stale sweep entry so the zset cannot
		// accumulate ghosts the sweeper would fetch forever.
		if err := r.rdb.ZRem(ctx, r.sweepKey(), leaseID).Err(); err != nil {
			return false, fmt.Errorf("quota: sweep entry cleanup: %w", err)
		}
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("quota: lease lookup: %w", err)
	}
	res, err := r.releaseScript.Run(ctx, r.rdb,
		[]string{r.balanceKey(tenant), r.leaseKey(leaseID), r.sweepKey(), r.refundedKey(tenant)},
		leaseID, string(state), leaseAuditTTL.Milliseconds(),
	).Int64()
	if err != nil {
		return false, fmt.Errorf("quota: release script: %w", err)
	}
	return res == 1, nil
}

// Balance implements Ledger.
func (r *Redis) Balance(ctx context.Context, tenantID string) (int64, error) {
	bal, err := r.rdb.Get(ctx, r.balanceKey(tenantID)).Int64()
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

// TenantSnapshot implements the reconcile SnapshotSource: the balance,
// the identity totals and the correction epoch read in one pipeline. A
// tenant without a balance key yields a nil snapshot (nothing
// provisioned).
func (r *Redis) TenantSnapshot(ctx context.Context, tenantID string, takenAt time.Time) (*Snapshot, error) {
	pipe := r.rdb.Pipeline()
	balCmd := pipe.Get(ctx, r.balanceKey(tenantID))
	consumedCmd := pipe.Get(ctx, r.consumedKey(tenantID))
	refundedCmd := pipe.Get(ctx, r.refundedKey(tenantID))
	debitedCmd := pipe.Get(ctx, r.debitedKey(tenantID))
	epochCmd := pipe.Get(ctx, r.epochKey(tenantID))
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("quota: snapshot read: %w", err)
	}

	if errors.Is(balCmd.Err(), redis.Nil) {
		return nil, nil
	}
	balance, err := balCmd.Int64()
	if err != nil {
		return nil, fmt.Errorf("quota: snapshot balance: %w", err)
	}
	consumed := counterValue(consumedCmd)
	refunded := counterValue(refundedCmd)
	debited := counterValue(debitedCmd)
	epoch := counterValue(epochCmd)
	return &Snapshot{
		TenantID: tenantID,
		Balance:  balance,
		Consumed: consumed,
		Refunded: refunded,
		Debited:  debited,
		Epoch:    epoch,
		TakenAt:  takenAt,
	}, nil
}

// counterValue reads a lifetime counter; a missing key counts zero.
func counterValue(cmd *redis.StringCmd) int64 {
	v, err := cmd.Int64()
	if err != nil {
		return 0
	}
	return v
}

// SweepOnce reclaims expired RESERVED leases through the release
// script, which moves each one to EXPIRED and refunds its amount
// exactly once. It returns how many leases were reclaimed.
func (r *Redis) SweepOnce(ctx context.Context, now time.Time, limit int) (int, error) {
	ids, err := r.rdb.ZRangeByScore(ctx, r.sweepKey(), &redis.ZRangeBy{
		Min:   "-inf",
		Max:   fmt.Sprintf("%d", now.UnixMilli()),
		Count: int64(limit),
	}).Result()
	if err != nil {
		return 0, fmt.Errorf("quota: sweep scan: %w", err)
	}
	expired := 0
	for _, id := range ids {
		moved, err := r.release(ctx, id, LeaseStateExpired)
		if err != nil {
			return expired, err
		}
		if moved {
			expired++
		}
	}
	return expired, nil
}
