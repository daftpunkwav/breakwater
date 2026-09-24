/**
 * @file redis_test
 * @description Redis ledger tests over miniredis: script semantics,
 * sweeper convergence and the concurrent-drain reconciliation evidence
 * (invariants I3/I9, run under -race).
 */
package quota

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestLedger(t *testing.T) (*Redis, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return NewRedis(client), mr
}

func TestRedisReserveSettle(t *testing.T) {
	t.Parallel()
	r, _ := newTestLedger(t)
	ctx := context.Background()

	if err := r.SetBalance(ctx, "t", 1000); err != nil {
		t.Fatalf("seed: %v", err)
	}
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
	r, _ := newTestLedger(t)
	ctx := context.Background()

	if err := r.SetBalance(ctx, "t", 100); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := r.Reserve(ctx, "t", 200); !errors.Is(err, ErrInsufficientBalance) {
		t.Fatalf("err = %v, want ErrInsufficientBalance", err)
	}
	// Unprovisioned tenants are denied.
	if _, err := r.Reserve(ctx, "ghost", 1); !errors.Is(err, ErrInsufficientBalance) {
		t.Fatalf("unprovisioned err = %v", err)
	}
}

func TestRedisSweeperReclaimsAbandonedLeases(t *testing.T) {
	t.Parallel()
	r, mr := newTestLedger(t)
	ctx := context.Background()

	if err := r.SetBalance(ctx, "t", 1000); err != nil {
		t.Fatalf("seed: %v", err)
	}
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

// TestRedisConcurrentDrainReconciles is the I3 evidence: concurrent
// reservations of a shared balance under -race, each settling with
// varying usage, must reconcile with zero error — the identity
// initial = final + consumed must hold exactly.
func TestRedisConcurrentDrainReconciles(t *testing.T) {
	t.Parallel()
	r, _ := newTestLedger(t)
	ctx := context.Background()

	const (
		initial    = int64(1_000_000)
		reserveAmt = int64(100)
		workers    = 64
		rounds     = 25
	)
	if err := r.SetBalance(ctx, "t", initial); err != nil {
		t.Fatalf("seed: %v", err)
	}

	var consumed atomic.Int64
	var wg sync.WaitGroup
	errs := make(chan error, workers*rounds)
	for w := range workers {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for round := range rounds {
				lease, err := r.Reserve(ctx, "t", reserveAmt)
				if err != nil {
					errs <- err
					continue
				}
				// Vary usage deterministically across the refund range.
				used := int64((worker + round) % 101)
				if err := r.Settle(ctx, lease.ID, used); err != nil {
					errs <- err
					continue
				}
				consumed.Add(used)
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent drain error: %v", err)
	}

	final, err := r.Balance(ctx, "t")
	if err != nil {
		t.Fatalf("balance: %v", err)
	}
	want := initial - consumed.Load()
	if final != want {
		t.Fatalf("reconciliation error: balance = %d, want %d (drift %d)",
			final, want, final-want)
	}
}
