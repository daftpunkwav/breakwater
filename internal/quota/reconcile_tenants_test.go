/**
 * @file reconcile_tenants_test
 * @description The reconcile loop's tenant isolation: one tenant's
 * failing ledger must skip only that tenant — the rest of the round
 * still snapshots, still checks, still detects drift.
 */
package quota

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// tenantFailingSource fails chosen tenants' snapshot reads and serves
// the rest from the wrapped source.
type tenantFailingSource struct {
	inner   SnapshotSource
	failFor map[string]bool
}

func (f *tenantFailingSource) TenantSnapshot(ctx context.Context, tenantID string, takenAt time.Time) (*Snapshot, error) {
	if f.failFor[tenantID] {
		return nil, errors.New("ledger poisoned")
	}
	return f.inner.TenantSnapshot(ctx, tenantID, takenAt)
}

// TestReconcileOnceIsolatesTenantFailures: a poisoned first tenant
// leaves the second fully reconciled — snapshot appended, drift still
// detected — with the failure aggregated in the returned error.
func TestReconcileOnceIsolatesTenantFailures(t *testing.T) {
	t.Parallel()
	source := &tenantFailingSource{
		inner:   &scriptedSnapshotSource{snap: &Snapshot{Balance: 90, Debited: 100, Refunded: 10, Epoch: 1}},
		failFor: map[string]bool{"poisoned": true},
	}
	store := newMemSnapshotStore()
	// The healthy tenant's prior reading: the round must still reach it.
	prev := Snapshot{TenantID: "healthy", Balance: 95, Debited: 100, Refunded: 5, Epoch: 1}
	if err := store.Append(context.Background(), prev); err != nil {
		t.Fatalf("seed: %v", err)
	}
	r := NewReconciler(source, staticTenants("poisoned", "healthy"), store)

	_, drifted, drifts, err := r.ReconcileOnce(context.Background())
	if err == nil || !strings.Contains(err.Error(), "poisoned") {
		t.Fatalf("err = %v, want the poisoned tenant's failure aggregated", err)
	}
	if drifted != 1 || len(drifts) != 1 || drifts[0].TenantID != "healthy" {
		t.Fatalf("drifted = %d drifts = %v, want the healthy tenant's drift still detected", drifted, drifts)
	}
}

// stampingSnapshotSource serves one shared snapshot reading, stamped
// with the tenant it was read for.
type stampingSnapshotSource struct{ snap Snapshot }

func (s *stampingSnapshotSource) TenantSnapshot(_ context.Context, tenantID string, takenAt time.Time) (*Snapshot, error) {
	snap := s.snap
	snap.TenantID = tenantID
	snap.TakenAt = takenAt
	return &snap, nil
}

// TestReconcileDiscoversTenantsEachRound pins the per-round discovery
// contract: a tenant provisioned after the reconciler started enters
// the snapshot chain from the next round on, instead of never being
// reconciled because a startup-time list missed it.
func TestReconcileDiscoversTenantsEachRound(t *testing.T) {
	t.Parallel()
	source := &stampingSnapshotSource{snap: Snapshot{Balance: 90, Debited: 100, Refunded: 10, Epoch: 1}}
	store := newMemSnapshotStore()

	discovered := []string{"early"}
	r := NewReconciler(source, func(context.Context) ([]string, error) {
		return discovered, nil
	}, store)

	// Round 1: the late tenant does not exist yet.
	if _, _, _, err := r.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("round 1: %v", err)
	}
	if _, ok := store.latest["late"]; ok {
		t.Fatal("round 1 snapshotted a tenant that was not yet provisioned")
	}

	// The admin surface provisions a new tenant between rounds.
	discovered = append(discovered, "late")

	// Round 2: first sight takes the baseline snapshot — the interval
	// is skipped by the correction rule, but the tenant must be in the
	// chain from here on.
	if _, _, _, err := r.ReconcileOnce(context.Background()); err != nil {
		t.Fatalf("round 2: %v", err)
	}
	if _, ok := store.latest["late"]; !ok {
		t.Fatal("round 2 never discovered the newly provisioned tenant")
	}

	// Round 3: the newly provisioned tenant is fully reconciled now.
	checked, _, _, err := r.ReconcileOnce(context.Background())
	if err != nil {
		t.Fatalf("round 3: %v", err)
	}
	if checked != 2 {
		t.Fatalf("round 3 checked = %d, want both tenants reconciled", checked)
	}
}
