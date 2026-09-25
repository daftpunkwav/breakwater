/**
 * @file reconcile_errors_test
 * @description ReconcileOnce error propagation: a snapshot source
 * failure, a failed latest read and a failed append each surface
 * instead of being read as a clean interval.
 */
package quota

import (
	"context"
	"errors"
	"testing"
	"time"
)

// failingSource fails every snapshot read.
type failingSource struct{ err error }

func (f failingSource) TenantSnapshot(context.Context, string, time.Time) (*Snapshot, error) {
	return nil, f.err
}

// failingStore wraps the in-memory store with an injected failure on
// one operation.
type failingStore struct {
	*memSnapshotStore
	latestErr error
	appendErr error
}

func (f *failingStore) Latest(ctx context.Context, tenantID string) (*Snapshot, error) {
	if f.latestErr != nil {
		return nil, f.latestErr
	}
	return f.memSnapshotStore.Latest(ctx, tenantID)
}

func (f *failingStore) Append(ctx context.Context, snap Snapshot) error {
	if f.appendErr != nil {
		return f.appendErr
	}
	return f.memSnapshotStore.Append(ctx, snap)
}

func TestReconcileOnceSurfacesSourceFailure(t *testing.T) {
	t.Parallel()
	sourceErr := errors.New("snapshot read: backend down")
	rec := NewReconciler(failingSource{err: sourceErr}, []string{"t"}, newMemSnapshotStore())

	_, _, _, err := rec.ReconcileOnce(context.Background())
	if !errors.Is(err, sourceErr) {
		t.Fatalf("err = %v, want the source failure surfaced", err)
	}
}

func TestReconcileOnceSurfacesLatestFailure(t *testing.T) {
	t.Parallel()
	latestErr := errors.New("latest: database down")
	store := &failingStore{memSnapshotStore: newMemSnapshotStore(), latestErr: latestErr}
	// The source returns a real snapshot so the Latest call is reached.
	source := &scriptedSnapshotSource{snap: &Snapshot{TenantID: "t", Epoch: 1}}
	rec := NewReconciler(source, []string{"t"}, store)
	if _, _, _, err := rec.ReconcileOnce(context.Background()); !errors.Is(err, latestErr) {
		t.Fatalf("err = %v, want the latest-read failure surfaced", err)
	}
}

func TestReconcileOnceSurfacesAppendFailure(t *testing.T) {
	t.Parallel()
	appendErr := errors.New("append: database down")
	store := &failingStore{memSnapshotStore: newMemSnapshotStore(), appendErr: appendErr}
	source := &scriptedSnapshotSource{snap: &Snapshot{TenantID: "t", Epoch: 1}}
	rec := NewReconciler(source, []string{"t"}, store)

	if _, _, _, err := rec.ReconcileOnce(context.Background()); !errors.Is(err, appendErr) {
		t.Fatalf("err = %v, want the append failure surfaced", err)
	}
}

// scriptedSnapshotSource answers every read with the same snapshot.
type scriptedSnapshotSource struct{ snap *Snapshot }

func (s *scriptedSnapshotSource) TenantSnapshot(_ context.Context, tenantID string, takenAt time.Time) (*Snapshot, error) {
	snap := *s.snap
	snap.TakenAt = takenAt
	return &snap, nil
}
