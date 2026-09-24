/**
 * @file memory_test
 * @description In-memory ledger semantics: reserve/settle/cancel
 * transitions, no over-draft, sweeper convergence (I3/I9).
 */
package quota

import (
	"context"
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
