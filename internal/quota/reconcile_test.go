/**
 * @file reconcile_test
 * @description Reconciliation protocol tests over miniredis: the
 * balance identity must hold across intervals (I3) — including
 * in-flight reservations and overage settles, which are ledger facts,
 * not drift — manual corrections skip the check by design, and injected
 * drift is detected and reported.
 */
package quota

import (
	"context"
	"testing"
)

// memSnapshotStore is the in-memory SnapshotStore the tests drive.
type memSnapshotStore struct {
	latest map[string]*Snapshot
}

func newMemSnapshotStore() *memSnapshotStore {
	return &memSnapshotStore{latest: map[string]*Snapshot{}}
}

func (m *memSnapshotStore) Latest(_ context.Context, tenantID string) (*Snapshot, error) {
	return m.latest[tenantID], nil
}

func (m *memSnapshotStore) Append(_ context.Context, snap Snapshot) error {
	copied := snap
	m.latest[snap.TenantID] = &copied
	return nil
}

func TestReconcileCleanIntervalsHaveNoDrift(t *testing.T) {
	t.Parallel()
	r, _ := newTestLedger(t)
	ctx := context.Background()

	if err := r.SetBalance(ctx, "t", 1_000_000); err != nil {
		t.Fatalf("seed: %v", err)
	}

	store := newMemSnapshotStore()
	rec := NewReconciler(r, []string{"t"}, store)

	// Interval 1: reserve and settle some usage.
	lease, err := r.Reserve(ctx, "t", 100)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if err := r.Settle(ctx, lease.ID, 60); err != nil {
		t.Fatalf("settle: %v", err)
	}
	if _, _, drifts, err := rec.ReconcileOnce(ctx); err != nil || len(drifts) != 0 {
		t.Fatalf("interval 1: checked drifts = %v err = %v (first sight skips by design)", drifts, err)
	}

	// Interval 2: reserve, cancel and settle with varied usage.
	leaseA, _ := r.Reserve(ctx, "t", 100)
	if err := r.Cancel(ctx, leaseA.ID); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	leaseB, _ := r.Reserve(ctx, "t", 100)
	if err := r.Settle(ctx, leaseB.ID, 30); err != nil {
		t.Fatalf("settle: %v", err)
	}
	checked, drifted, drifts, err := rec.ReconcileOnce(ctx)
	if err != nil {
		t.Fatalf("interval 2: %v", err)
	}
	if checked != 1 || drifted != 0 || len(drifts) != 0 {
		t.Fatalf("checked = %d drifted = %d drifts = %v, want a clean interval", checked, drifted, drifts)
	}
}

// TestReconcileToleratesInFlightLeasesAndOverage pins the identity's
// counter pairing: a lease still RESERVED at a snapshot boundary and an
// overage settle (usage above the reservation) move the balance and the
// debited/refunded counters together — ledger facts, never drift.
func TestReconcileToleratesInFlightLeasesAndOverage(t *testing.T) {
	t.Parallel()
	r, mr := newTestLedger(t)
	ctx := context.Background()

	if err := r.SetBalance(ctx, "t", 1_000_000); err != nil {
		t.Fatalf("seed: %v", err)
	}
	store := newMemSnapshotStore()
	rec := NewReconciler(r, []string{"t"}, store)

	if _, _, _, err := rec.ReconcileOnce(ctx); err != nil {
		t.Fatalf("baseline: %v", err)
	}

	// Interval 1: a lease still RESERVED at the snapshot instant — its
	// balance debit must not read as drift.
	inFlight, err := r.Reserve(ctx, "t", 100)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if _, _, drifts, err := rec.ReconcileOnce(ctx); err != nil || len(drifts) != 0 {
		t.Fatalf("in-flight interval drifts = %v err = %v, want none", drifts, err)
	}

	// Interval 2: that lease settles with a refund after the boundary.
	if err := r.Settle(ctx, inFlight.ID, 60); err != nil {
		t.Fatalf("settle: %v", err)
	}
	if _, _, drifts, err := rec.ReconcileOnce(ctx); err != nil || len(drifts) != 0 {
		t.Fatalf("settle-after-boundary drifts = %v err = %v, want none", drifts, err)
	}

	// Interval 3: an overage settle (usage above the reservation, no
	// refund) and a cancelled lease.
	over, err := r.Reserve(ctx, "t", 50)
	if err != nil {
		t.Fatalf("overage reserve: %v", err)
	}
	if err := r.Settle(ctx, over.ID, 80); err != nil {
		t.Fatalf("overage settle: %v", err)
	}
	released, err := r.Reserve(ctx, "t", 70)
	if err != nil {
		t.Fatalf("release reserve: %v", err)
	}
	if err := r.Cancel(ctx, released.ID); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if _, _, drifts, err := rec.ReconcileOnce(ctx); err != nil || len(drifts) != 0 {
		t.Fatalf("overage/release interval drifts = %v err = %v, want none", drifts, err)
	}

	// The exact identity still catches corruption from outside the
	// ledger protocol.
	_ = mr.Set("bw:quota:bal:t", "999999")
	_, drifted, drifts, err := rec.ReconcileOnce(ctx)
	if err != nil || drifted != 1 || len(drifts) != 1 || drifts[0].TenantID != "t" {
		t.Fatalf("drifted = %d drifts = %v err = %v, want the injected drift reported", drifted, drifts, err)
	}
}

func TestReconcileDetectsInjectedDrift(t *testing.T) {
	t.Parallel()
	r, mr := newTestLedger(t)
	ctx := context.Background()

	if err := r.SetBalance(ctx, "t", 100_000); err != nil {
		t.Fatalf("seed: %v", err)
	}
	store := newMemSnapshotStore()
	rec := NewReconciler(r, []string{"t"}, store)

	lease, _ := r.Reserve(ctx, "t", 100)
	if err := r.Settle(ctx, lease.ID, 40); err != nil {
		t.Fatalf("settle: %v", err)
	}
	if _, _, _, err := rec.ReconcileOnce(ctx); err != nil {
		t.Fatalf("baseline: %v", err)
	}

	lease2, _ := r.Reserve(ctx, "t", 50)
	if err := r.Settle(ctx, lease2.ID, 20); err != nil {
		t.Fatalf("settle 2: %v", err)
	}
	// Corrupt the balance outside the ledger protocol: the identity
	// must catch what the scripts cannot prevent.
	_ = mr.Set("bw:quota:bal:t", "99999")

	checked, drifted, drifts, err := rec.ReconcileOnce(ctx)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if checked != 1 || drifted != 1 || len(drifts) != 1 || drifts[0].TenantID != "t" || drifts[0].Drift == 0 {
		t.Fatalf("checked = %d drifted = %d drifts = %+v, want the injected drift reported", checked, drifted, drifts)
	}
}

func TestReconcileSkipsIntervalAcrossManualCorrection(t *testing.T) {
	t.Parallel()
	r, _ := newTestLedger(t)
	ctx := context.Background()

	if err := r.SetBalance(ctx, "t", 10_000); err != nil {
		t.Fatalf("seed: %v", err)
	}
	store := newMemSnapshotStore()
	rec := NewReconciler(r, []string{"t"}, store)

	if _, _, _, err := rec.ReconcileOnce(ctx); err != nil {
		t.Fatalf("baseline: %v", err)
	}

	// A top-up between intervals moves the balance without touching the
	// totals: the epoch must make the reconciler skip this interval.
	if err := r.SetBalance(ctx, "t", 50_000); err != nil {
		t.Fatalf("top-up: %v", err)
	}
	checked, drifted, drifts, err := rec.ReconcileOnce(ctx)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if checked != 0 || drifted != 0 || len(drifts) != 0 {
		t.Fatalf("checked = %d drifted = %d drifts = %v, want the corrected interval skipped", checked, drifted, drifts)
	}
}

func TestReconcileSkipsUnprovisionedTenants(t *testing.T) {
	t.Parallel()
	r, _ := newTestLedger(t)
	store := newMemSnapshotStore()
	rec := NewReconciler(r, []string{"ghost"}, store)

	checked, drifted, drifts, err := rec.ReconcileOnce(context.Background())
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if checked != 0 || drifted != 0 || len(drifts) != 0 {
		t.Fatalf("checked = %d drifted = %d, want a silent skip", checked, drifted)
	}
}
