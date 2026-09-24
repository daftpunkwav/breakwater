/**
 * @file memory_test
 * @description In-memory ledger semantics: reserve/settle/cancel
 * transitions, no over-draft, sweeper convergence (I3/I9).
 */
package quota

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestMemoryReserveSettleRefundsDifference(t *testing.T) {
	t.Parallel()
	m := NewMemory()
	ctx := context.Background()
	m.SetBalance("t", 1000)

	lease, err := m.Reserve(ctx, "t", 400)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if bal, _ := m.Balance(ctx, "t"); bal != 600 {
		t.Fatalf("balance after reserve = %d, want 600", bal)
	}
	if err := m.Settle(ctx, lease.ID, 150); err != nil {
		t.Fatalf("settle: %v", err)
	}
	if bal, _ := m.Balance(ctx, "t"); bal != 850 {
		t.Fatalf("balance after settle = %d, want 850", bal)
	}
}

func TestMemorySettleNeverSurcharges(t *testing.T) {
	t.Parallel()
	m := NewMemory()
	ctx := context.Background()
	m.SetBalance("t", 1000)

	lease, _ := m.Reserve(ctx, "t", 100)
	if err := m.Settle(ctx, lease.ID, 500); err != nil {
		t.Fatalf("settle with overuse: %v", err)
	}
	if bal, _ := m.Balance(ctx, "t"); bal != 900 {
		t.Fatalf("balance = %d, want 900: settle must not surcharge", bal)
	}
}

func TestMemoryInsufficientBalanceAndNoOverDraft(t *testing.T) {
	t.Parallel()
	m := NewMemory()
	ctx := context.Background()
	m.SetBalance("t", 100)

	if _, err := m.Reserve(ctx, "t", 200); err != ErrInsufficientBalance {
		t.Fatalf("oversized reserve err = %v, want ErrInsufficientBalance", err)
	}
	if bal, _ := m.Balance(ctx, "t"); bal != 100 {
		t.Fatalf("balance = %d, want untouched 100", bal)
	}
	// Unprovisioned tenants are denied, never created from thin air.
	if _, err := m.Reserve(ctx, "ghost", 1); err != ErrInsufficientBalance {
		t.Fatalf("unprovisioned reserve err = %v", err)
	}
}

func TestMemoryCancelReleasesFullReservation(t *testing.T) {
	t.Parallel()
	m := NewMemory()
	ctx := context.Background()
	m.SetBalance("t", 100)

	lease, _ := m.Reserve(ctx, "t", 100)
	if err := m.Cancel(ctx, lease.ID); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if bal, _ := m.Balance(ctx, "t"); bal != 100 {
		t.Fatalf("balance = %d, want 100 after cancel", bal)
	}
}

func TestMemoryTerminalTransitionsAreNoOps(t *testing.T) {
	t.Parallel()
	m := NewMemory()
	ctx := context.Background()
	m.SetBalance("t", 300)

	lease, _ := m.Reserve(ctx, "t", 300)
	if err := m.Settle(ctx, lease.ID, 100); err != nil {
		t.Fatalf("settle: %v", err)
	}
	// A late settle (sweeper raced the client) must not double-refund.
	if err := m.Settle(ctx, lease.ID, 100); err != nil {
		t.Fatalf("late settle: %v", err)
	}
	if err := m.Cancel(ctx, lease.ID); err != nil {
		t.Fatalf("cancel after settle: %v", err)
	}
	if bal, _ := m.Balance(ctx, "t"); bal != 200 {
		t.Fatalf("balance = %d, want 200: terminal transitions must be no-ops", bal)
	}
}

func TestMemorySweeperReclaimsAbandonedLeases(t *testing.T) {
	t.Parallel()
	m := NewMemory()
	ctx := context.Background()
	m.SetBalance("t", 1000)

	if _, err := m.Reserve(ctx, "t", 400); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	// Nothing settled: the holder "crashed".
	expired, err := m.SweepOnce(ctx, time.Now(), 100)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if expired != 0 {
		t.Fatalf("expired %d fresh leases, want 0", expired)
	}

	future := time.Now().Add(defaultLeaseTTL + time.Second)
	expired, err = m.SweepOnce(ctx, future, 100)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if expired != 1 {
		t.Fatalf("expired = %d, want 1", expired)
	}
	if bal, _ := m.Balance(ctx, "t"); bal != 1000 {
		t.Fatalf("balance = %d, want fully refunded 1000", bal)
	}
	// The reclaimed lease is terminal: a late settle is a no-op.
}

// TestMemoryBalanceUnknownTenant locks the Balance error contract: an
// unprovisioned tenant is reported, not read as zero, so the admin API
// can tell "no ledger" apart from "drained".
func TestMemoryBalanceUnknownTenant(t *testing.T) {
	t.Parallel()
	m := NewMemory()
	m.SetBalance("t", 7)
	ctx := context.Background()

	if _, err := m.Balance(ctx, "ghost"); !errors.Is(err, ErrUnknownTenant) {
		t.Fatalf("unprovisioned balance err = %v, want ErrUnknownTenant", err)
	}
	bal, err := m.Balance(ctx, "t")
	if err != nil || bal != 7 {
		t.Fatalf("balance = %d err = %v, want 7/nil", bal, err)
	}
}

// TestMemoryTerminalLeasesPurgeAfterAuditWindow pins the bounded-growth
// rule: terminal records survive the audit window (matching the Redis
// backend) and are purged by the sweeper afterwards — without touching
// balances again, and with a late settle of a purged lease surfaced.
func TestMemoryTerminalLeasesPurgeAfterAuditWindow(t *testing.T) {
	t.Parallel()
	m := NewMemory()
	ctx := context.Background()
	m.SetBalance("t", 1000)

	first, err := m.Reserve(ctx, "t", 100)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	second, err := m.Reserve(ctx, "t", 100)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if err := m.Settle(ctx, first.ID, 40); err != nil {
		t.Fatalf("settle: %v", err)
	}
	if err := m.Cancel(ctx, second.ID); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	// 1000 - 100 - 100 + 60 (settle refund) + 100 (cancel) = 960.
	if bal, _ := m.Balance(ctx, "t"); bal != 960 {
		t.Fatalf("balance = %d, want 960", bal)
	}
	if len(m.leases) != 2 {
		t.Fatalf("retained leases = %d, want 2 inside the audit window", len(m.leases))
	}

	// Sweeping inside the audit window keeps the records for audit.
	midway := time.Now().Add(defaultLeaseTTL + time.Second)
	expired, err := m.SweepOnce(ctx, midway, 100)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if expired != 0 {
		t.Fatalf("expired = %d, want 0: terminal records are not reclaims", expired)
	}
	if len(m.leases) != 2 {
		t.Fatalf("retained leases = %d, want 2: audit window not elapsed", len(m.leases))
	}

	// Past the audit window the sweeper purges them; balances stay put.
	after := time.Now().Add(defaultLeaseTTL + leaseAuditTTL + time.Second)
	expired, err = m.SweepOnce(ctx, after, 100)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if expired != 0 {
		t.Fatalf("expired = %d, want 0: a purge is not a reclaim", expired)
	}
	if len(m.leases) != 0 {
		t.Fatalf("retained leases = %d, want 0 after the audit window", len(m.leases))
	}
	if bal, _ := m.Balance(ctx, "t"); bal != 960 {
		t.Fatalf("balance = %d, want unchanged 960: purges never refund", bal)
	}

	// A settle arriving after the purge is surfaced, never silent.
	if err := m.Settle(ctx, first.ID, 40); err == nil {
		t.Fatal("settle of a purged lease must surface, not silently no-op")
	}
}

// TestMemoryConcurrentDrainReconciles is the memory backend's I3
// evidence, mirroring the Redis script test: concurrent reserve/settle
// rounds against a shared balance under -race must reconcile exactly —
// initial = final + consumed, zero drift.
func TestMemoryConcurrentDrainReconciles(t *testing.T) {
	t.Parallel()
	m := NewMemory()
	ctx := context.Background()

	const (
		initial    = int64(1_000_000)
		reserveAmt = int64(100)
		workers    = 64
		rounds     = 25
	)
	m.SetBalance("t", initial)

	var consumed atomic.Int64
	var wg sync.WaitGroup
	errs := make(chan error, workers*rounds)
	for w := range workers {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for round := range rounds {
				lease, err := m.Reserve(ctx, "t", reserveAmt)
				if err != nil {
					errs <- err
					continue
				}
				// Vary usage deterministically across the refund range.
				used := int64((worker + round) % 101)
				if err := m.Settle(ctx, lease.ID, used); err != nil {
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

	final, err := m.Balance(ctx, "t")
	if err != nil {
		t.Fatalf("balance: %v", err)
	}
	if want := initial - consumed.Load(); final != want {
		t.Fatalf("reconciliation error: balance = %d, want %d (drift %d)",
			final, want, final-want)
	}
}
