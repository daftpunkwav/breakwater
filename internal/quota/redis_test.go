/**
 * @file redis_test
 * @description Redis ledger tests over miniredis: script semantics,
 * sweeper convergence and the concurrent-drain reconciliation evidence
 * (run under -race).
 */
package quota

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// newMiniredisClient starts miniredis and binds a client to it; both
// are torn down when the test ends.
func newMiniredisClient(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return mr, client
}

func newTestLedger(t *testing.T) (*Redis, *miniredis.Miniredis) {
	t.Helper()
	mr, client := newMiniredisClient(t)
	return NewRedis(client, "", 10*time.Minute), mr
}

func TestRedisReserveSettle(t *testing.T) {
	t.Parallel()
	r, _, ctx := newSeededRedisLedger(t, 1000)

	lease, err := r.Reserve(ctx, "t", 400)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if bal, _ := r.Balance(ctx, "t"); bal != 600 {
		t.Fatalf("balance = %d, want 600", bal)
	}
	if err := r.Settle(ctx, lease.ID, 150); err != nil {
		t.Fatalf("settle: %v", err)
	}
	if bal, _ := r.Balance(ctx, "t"); bal != 850 {
		t.Fatalf("balance = %d, want 850", bal)
	}
}

func TestRedisInsufficientBalance(t *testing.T) {
	t.Parallel()
	r, _, ctx := newSeededRedisLedger(t, 100)

	if _, err := r.Reserve(ctx, "t", 200); !errors.Is(err, ErrInsufficientBalance) {
		t.Fatalf("err = %v, want ErrInsufficientBalance", err)
	}
	// Unprovisioned tenants are denied, and denied distinctly: the
	// script separates a missing balance key from a low one, and the Go
	// side must not collapse the two.
	if _, err := r.Reserve(ctx, "ghost", 1); !errors.Is(err, ErrUnknownTenant) {
		t.Fatalf("unprovisioned err = %v, want ErrUnknownTenant", err)
	}
	// An unprovisioned balance query is reported, not read as zero.
	if _, err := r.Balance(ctx, "ghost"); !errors.Is(err, ErrUnknownTenant) {
		t.Fatalf("unprovisioned balance err = %v, want ErrUnknownTenant", err)
	}
}

func TestRedisSweeperReclaimsAbandonedLeases(t *testing.T) {
	t.Parallel()
	r, mr, ctx := newSeededRedisLedger(t, 1000)

	lease, err := r.Reserve(ctx, "t", 400)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	// The clock jumps past the lease TTL without a settle: the holder
	// "crashed between reserve and settle".
	mr.SetTime(time.Now().Add(defaultLeaseTTL + time.Second))
	expired, err := r.SweepOnce(ctx, time.Now().Add(defaultLeaseTTL+time.Second), 100)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if expired != 1 {
		t.Fatalf("expired = %d, want 1", expired)
	}
	if bal, _ := r.Balance(ctx, "t"); bal != 1000 {
		t.Fatalf("balance = %d, want refunded 1000", bal)
	}
	// The reclaimed lease is terminal: a late settle must not refund twice.
	if err := r.Settle(ctx, lease.ID, 0); err != nil {
		t.Fatalf("late settle: %v", err)
	}
	if bal, _ := r.Balance(ctx, "t"); bal != 1000 {
		t.Fatalf("balance = %d after late settle, want 1000", bal)
	}
}

// TestRedisConcurrentDrainReconciles is the reconciliation evidence:
// concurrent
// reservations of a shared balance under -race, each settling with
// varying usage, must reconcile with zero error — the identity
// initial = final + consumed must hold exactly.
func TestRedisConcurrentDrainReconciles(t *testing.T) {
	t.Parallel()
	r, _ := newTestLedger(t)

	const initial = int64(1_000_000)
	consumed, final := runConcurrentDrain(t, r, initial, 100)
	if want := initial - consumed; final != want {
		t.Fatalf("reconciliation error: balance = %d, want %d (drift %d)",
			final, want, final-want)
	}
}

// TestRedisTerminalLeaseAuditWindowExpires locks the bounded-retention
// rule: a terminal lease record stays for the audit window, then
// expires, so the hash set cannot grow without bound; a late settle
// after expiry is a silent no-op.
func TestRedisTerminalLeaseAuditWindowExpires(t *testing.T) {
	t.Parallel()
	r, mr, ctx := newSeededRedisLedger(t, 1000)

	settled, err := r.Reserve(ctx, "t", 400)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	cancelled, err := r.Reserve(ctx, "t", 100)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if err := r.Settle(ctx, settled.ID, 150); err != nil {
		t.Fatalf("settle: %v", err)
	}
	if err := r.Cancel(ctx, cancelled.ID); err != nil {
		t.Fatalf("cancel: %v", err)
	}

	if !mr.Exists(r.leaseKey(settled.ID)) || !mr.Exists(r.leaseKey(cancelled.ID)) {
		t.Fatal("terminal records vanished before the audit window elapsed")
	}
	mr.FastForward(2 * leaseAuditTTL)
	if mr.Exists(r.leaseKey(settled.ID)) || mr.Exists(r.leaseKey(cancelled.ID)) {
		t.Fatal("terminal records survived the audit window")
	}
	// Balance accounting is untouched by the expiry.
	if bal, _ := r.Balance(ctx, "t"); bal != 850 {
		t.Fatalf("balance = %d, want 850", bal)
	}
	// A late settle after the record expired is a silent no-op.
	if err := r.Settle(ctx, settled.ID, 150); err != nil {
		t.Fatalf("late settle after expiry = %v, want nil no-op", err)
	}
}

// TestRedisSweepDropsGhostEntries locks the sweep convergence rule: a
// zset entry whose lease hash is already gone must be dropped by the
// sweep instead of being fetched forever.
func TestRedisSweepDropsGhostEntries(t *testing.T) {
	t.Parallel()
	r, _ := newTestLedger(t)
	ctx := context.Background()

	if err := r.rdb.ZAdd(ctx, r.sweepKey(), redis.Z{
		Score:  float64(time.Now().Add(-time.Hour).UnixMilli()),
		Member: "ghost-lease",
	}).Err(); err != nil {
		t.Fatalf("seed ghost: %v", err)
	}

	n, err := r.SweepOnce(ctx, time.Now(), 100)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if n != 0 {
		t.Fatalf("reclaimed = %d, want 0 (nothing to refund)", n)
	}
	if card := r.rdb.ZCard(ctx, r.sweepKey()).Val(); card != 0 {
		t.Fatalf("sweep zset cardinality = %d, want 0 (ghost dropped)", card)
	}
}

// TestRedisNamespaceIsolatesDeployments: two gateways pointed at one
// Redis must not see each other's money. A shared keyspace would let one
// environment spend and refund the other's balance.
func TestRedisNamespaceIsolatesDeployments(t *testing.T) {
	t.Parallel()
	mr, client := newMiniredisClient(t)
	ctx := context.Background()

	staging := NewRedis(client, "staging", 10*time.Minute)
	production := NewRedis(client, "production", 10*time.Minute)

	if err := production.SetBalance(ctx, "tenant", 1000); err != nil {
		t.Fatalf("provision production: %v", err)
	}
	// The tenant is unknown to the other deployment, not broke in it.
	if _, err := staging.Reserve(ctx, "tenant", 1); !errors.Is(err, ErrUnknownTenant) {
		t.Fatalf("staging reserve = %v, want ErrUnknownTenant", err)
	}
	if bal, err := production.Balance(ctx, "tenant"); err != nil || bal != 1000 {
		t.Fatalf("production balance = %d err = %v, want untouched 1000", bal, err)
	}
	if !mr.Exists("production:bw:quota:bal:tenant") {
		t.Fatalf("namespaced balance key missing; keys in the store: %v", mr.Keys())
	}
}

// TestRedisSettleClampsNegativeUsage pins the script against a hostile
// upstream: a negative usage count must never mint balance — the settle
// refunds at most what the lease reserved.
func TestRedisSettleClampsNegativeUsage(t *testing.T) {
	t.Parallel()
	r, _, ctx := newSeededRedisLedger(t, 1000)

	lease, err := r.Reserve(ctx, "t", 400)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if err := r.Settle(ctx, lease.ID, -25); err != nil {
		t.Fatalf("settle: %v", err)
	}
	// The clamp treats a negative count as "nothing consumed": the full
	// reservation refunds and the balance returns to its seed - never
	// past it, the way the unclamped arithmetic would (1425).
	if bal, _ := r.Balance(ctx, "t"); bal != 1000 {
		t.Fatalf("balance = %d, want the seed balance with no minted tokens (1000)", bal)
	}
}
