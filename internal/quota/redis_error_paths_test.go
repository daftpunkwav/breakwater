/**
 * @file redis_error_paths_test
 * @description The Redis ledger's guard rails over miniredis: health
 * reporting, balance provisioning, the terminal no-op surface and every
 * backend failure wrapped instead of masqueraded as a business outcome.
 */
package quota

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// closedLedger hands out a ledger whose backend has just gone down,
// together with a bounded context for the failing calls.
func closedLedger(t *testing.T) (*Redis, context.Context) {
	t.Helper()
	r, mr := newTestLedger(t)
	mr.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	return r, ctx
}

func TestRedisPingReportsHealth(t *testing.T) {
	t.Parallel()
	r, mr := newTestLedger(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := r.Ping(ctx); err != nil {
		t.Fatalf("ping against a live server: %v", err)
	}
	mr.Close()
	if err := r.Ping(ctx); err == nil {
		t.Fatal("ping against a closed server must report unhealthy")
	}
}

func TestRedisEnsureBalanceProvisionsOnce(t *testing.T) {
	t.Parallel()
	r, _ := newTestLedger(t)
	ctx := context.Background()

	created, err := r.EnsureBalance(ctx, "t", 500)
	if err != nil || !created {
		t.Fatalf("first ensure = %v, %v; want created", created, err)
	}
	again, err := r.EnsureBalance(ctx, "t", 999)
	if err != nil || again {
		t.Fatalf("second ensure = %v, %v; want an absorbed no-op", again, err)
	}
	if bal, err := r.Balance(ctx, "t"); err != nil || bal != 500 {
		t.Fatalf("balance = %d, %v; want the original 500 untouched", bal, err)
	}
}

func TestRedisBackendFailuresSurface(t *testing.T) {
	t.Parallel()
	r, ctx := closedLedger(t)

	if _, err := r.Reserve(ctx, "t", 100); err == nil || !strings.Contains(err.Error(), "reserve script") {
		t.Fatalf("reserve err = %v, want the wrapped script failure", err)
	}
	if err := r.Settle(ctx, "lease-1", 10); err == nil || !strings.Contains(err.Error(), "settle lease lookup") {
		t.Fatalf("settle err = %v, want the wrapped lookup failure", err)
	}
	if _, err := r.terminate(ctx, "lease-1", LeaseStateExpired); err == nil || !strings.Contains(err.Error(), "lease lookup") {
		t.Fatalf("terminate err = %v, want the wrapped lookup failure", err)
	}
	if err := r.Cancel(ctx, "lease-1"); err == nil || !strings.Contains(err.Error(), "lease lookup") {
		t.Fatalf("cancel err = %v, want the wrapped lookup failure", err)
	}
	if _, err := r.Balance(ctx, "t"); err == nil || !strings.Contains(err.Error(), "quota: balance") {
		t.Fatalf("balance err = %v, want the wrapped read failure", err)
	}
	if _, err := r.EnsureBalance(ctx, "t", 100); err == nil || !strings.Contains(err.Error(), "ensure balance") {
		t.Fatalf("ensure err = %v, want the wrapped provisioning failure", err)
	}
	if _, err := r.TenantSnapshot(ctx, "t", time.Now()); err == nil || !strings.Contains(err.Error(), "snapshot read") {
		t.Fatalf("snapshot err = %v, want the wrapped pipeline failure", err)
	}
	if _, err := r.SweepOnce(ctx, time.Now(), 100); err == nil || !strings.Contains(err.Error(), "sweep scan") {
		t.Fatalf("sweep err = %v, want the wrapped scan failure", err)
	}
}

func TestRedisSnapshotReportsUnparseableBalance(t *testing.T) {
	t.Parallel()
	r, mr := newTestLedger(t)
	ctx := context.Background()

	// A balance corrupted outside the ledger protocol must surface as a
	// read failure, never as a zero or a skip.
	_ = mr.Set(balanceKey("t"), "not-a-number")
	if _, err := r.TenantSnapshot(ctx, "t", time.Now()); err == nil ||
		!strings.Contains(err.Error(), "snapshot balance") {
		t.Fatalf("err = %v, want the balance parse failure", err)
	}
}

func TestRedisCancelUnknownLeaseIsSurfacedNoOp(t *testing.T) {
	t.Parallel()
	r, _ := newTestLedger(t)
	ctx := context.Background()

	// A lease whose record is gone (audit window elapsed) cancels as a
	// logged no-op that also drops the stale sweep entry.
	if err := r.rdb.ZAdd(ctx, sweepKey(), redis.Z{
		Score:  float64(time.Now().Add(-time.Hour).UnixMilli()),
		Member: "vanished-lease",
	}).Err(); err != nil {
		t.Fatalf("seed sweep entry: %v", err)
	}
	if err := r.Cancel(ctx, "vanished-lease"); err != nil {
		t.Fatalf("cancel of a vanished lease: %v", err)
	}
	if card := r.rdb.ZCard(ctx, sweepKey()).Val(); card != 0 {
		t.Fatalf("stale sweep entry survived: cardinality %d", card)
	}
}

func TestRedisSetBalanceBumpsEpoch(t *testing.T) {
	t.Parallel()
	r, _ := newTestLedger(t)
	ctx := context.Background()

	// An unprovisioned tenant has no snapshot at all.
	snap, err := r.TenantSnapshot(ctx, "t", time.Now())
	if err != nil || snap != nil {
		t.Fatalf("unprovisioned snapshot = %+v, %v; want nil/nil", snap, err)
	}
	if err := r.SetBalance(ctx, "t", 1000); err != nil {
		t.Fatalf("set balance: %v", err)
	}
	snap, err = r.TenantSnapshot(ctx, "t", time.Now())
	if err != nil || snap == nil {
		t.Fatalf("snapshot after provisioning = %+v, %v", snap, err)
	}
	if snap.Balance != 1000 || snap.Epoch != 1 {
		t.Fatalf("snapshot = balance %d epoch %d, want 1000/1", snap.Balance, snap.Epoch)
	}
}
