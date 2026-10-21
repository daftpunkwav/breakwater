/**
 * @file memory_test
 * @description In-memory ledger semantics: reserve/settle/cancel
 * transitions, no over-draft, sweeper convergence.
 */
package quota

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMemoryReserveSettleRefundsDifference(t *testing.T) {
	t.Parallel()
	m, ctx := newSeededMemory(t, 1000)

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
	m, ctx := newSeededMemory(t, 1000)

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
	m, ctx := newSeededMemory(t, 100)

	if _, err := m.Reserve(ctx, "t", 200); err != ErrInsufficientBalance {
		t.Fatalf("oversized reserve err = %v, want ErrInsufficientBalance", err)
	}
	if bal, _ := m.Balance(ctx, "t"); bal != 100 {
		t.Fatalf("balance = %d, want untouched 100", bal)
	}
	// Unprovisioned tenants are denied, never created from thin air —
	// and denied distinctly, so a missing provisioning record is not
	// reported to a client as a spending limit.
	if _, err := m.Reserve(ctx, "ghost", 1); err != ErrUnknownTenant {
		t.Fatalf("unprovisioned reserve err = %v, want ErrUnknownTenant", err)
	}
}

func TestMemoryCancelReleasesFullReservation(t *testing.T) {
	t.Parallel()
	m, ctx := newSeededMemory(t, 100)

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
	m, ctx := newSeededMemory(t, 300)

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
	m, ctx := newSeededMemory(t, 1000)

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

// TestMemorySweepOnceBoundsItsScan pins the per-pass scan ceiling: one
// SweepOnce walks at most scanBatch leases, whatever the ledger holds.
// The bound is what keeps the lock's residency proportional to the
// batch instead of the ledger — a sweep that walked everything would
// turn one abandoned-lease backlog into a stop-the-world pause on every
// tick.
func TestMemorySweepOnceBoundsItsScan(t *testing.T) {
	t.Parallel()
	m := NewMemory()
	ctx := context.Background()
	if err := m.SetBalance(ctx, "t", int64(scanBatch+50)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	for i := 0; i < scanBatch+50; i++ {
		if _, err := m.Reserve(ctx, "t", 1); err != nil {
			t.Fatalf("reserve %d: %v", i, err)
		}
	}
	// Every lease is abandoned; the generous limit makes the scan
	// ceiling the binding constraint, not the expiry budget.
	expired, err := m.SweepOnce(ctx, time.Now().Add(defaultLeaseTTL+time.Second), scanBatch+50)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if expired != scanBatch {
		t.Fatalf("expired = %d, want exactly the %d-lease scan batch", expired, scanBatch)
	}
	// The refund says the same thing from the outside: exactly the
	// batch's worth of reservations was released, the 50 past the batch
	// still hold theirs.
	if bal, _ := m.Balance(ctx, "t"); bal != int64(scanBatch) {
		t.Fatalf("balance = %d, want exactly the batch's %d refunded", bal, scanBatch)
	}
	// The remainder expires on the next pass: bounding the scan delays
	// the reclaim, it never skips it.
	expired, err = m.SweepOnce(ctx, time.Now().Add(defaultLeaseTTL+time.Second), scanBatch+50)
	if err != nil {
		t.Fatalf("second sweep: %v", err)
	}
	if expired != 50 {
		t.Fatalf("second sweep expired = %d, want the remaining 50", expired)
	}
}

// TestMemoryBalanceUnknownTenant locks the Balance error contract: an
// unprovisioned tenant is reported, not read as zero, so the admin API
// can tell "no ledger" apart from "drained".
func TestMemoryBalanceUnknownTenant(t *testing.T) {
	t.Parallel()
	m := NewMemory()
	if err := m.SetBalance(context.Background(), "t", 7); err != nil {
		t.Fatalf("seed: %v", err)
	}
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
	m, ctx := newSeededMemory(t, 1000)

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

// TestMemoryConcurrentDrainReconciles is the memory backend's
// reconciliation evidence, mirroring the Redis script test: concurrent
// reserve/settle
// rounds against a shared balance under -race must reconcile exactly —
// initial = final + consumed, zero drift.
func TestMemoryConcurrentDrainReconciles(t *testing.T) {
	t.Parallel()
	m := NewMemory()

	const initial = int64(1_000_000)
	consumed, final := runConcurrentDrain(t, m, initial, 100)
	if want := initial - consumed; final != want {
		t.Fatalf("reconciliation error: balance = %d, want %d (drift %d)",
			final, want, final-want)
	}
}

// TestWithLeaseTTLGovernsReclaim: the in-memory ledger honours the
// horizon it was given, which is what lets the assembly size the reclaim
// window from the request budget instead of a package constant.
func TestWithLeaseTTLGovernsReclaim(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	m := NewMemory().WithLeaseTTL(time.Minute)
	if err := m.SetBalance(ctx, "t", 100); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if _, err := m.Reserve(ctx, "t", 100); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	start := time.Now()
	if expired, err := m.SweepOnce(ctx, start.Add(30*time.Second), 10); err != nil || expired != 0 {
		t.Fatalf("sweep before the horizon expired %d (err %v), want 0", expired, err)
	}
	expired, err := m.SweepOnce(ctx, start.Add(2*time.Minute), 10)
	if err != nil || expired != 1 {
		t.Fatalf("sweep past the horizon expired %d (err %v), want 1", expired, err)
	}
	if bal, _ := m.Balance(ctx, "t"); bal != 100 {
		t.Fatalf("balance = %d, want the reservation refunded", bal)
	}
	// A non-positive value keeps the package default rather than
	// disabling the sweep.
	if got := NewMemory().WithLeaseTTL(0).leaseTTL; got != defaultLeaseTTL {
		t.Fatalf("leaseTTL = %v, want the default %v", got, defaultLeaseTTL)
	}
}
