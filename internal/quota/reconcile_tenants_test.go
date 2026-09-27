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
	r := NewReconciler(source, []string{"poisoned", "healthy"}, store)

	_, drifted, drifts, err := r.ReconcileOnce(context.Background())
	if err == nil || !strings.Contains(err.Error(), "poisoned") {
		t.Fatalf("err = %v, want the poisoned tenant's failure aggregated", err)
	}
	if drifted != 1 || len(drifts) != 1 || drifts[0].TenantID != "healthy" {
		t.Fatalf("drifted = %d drifts = %v, want the healthy tenant's drift still detected", drifted, drifts)
	}
}
