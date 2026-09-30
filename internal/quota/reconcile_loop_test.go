/**
 * @file reconcile_loop_test
 * @description The background reconcile loop: drifts fire the callback,
 * a source failure is logged and survived, and cancellation stops it.
 */
package quota

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// driftingSource answers each snapshot read with a balance that falls
// 20 tokens while the debited total only grows by 10: a -10 drift per
// checked interval, unless switched to a failure.
type driftingSource struct {
	mu    sync.Mutex
	fails bool
	reads int
}

func (s *driftingSource) TenantSnapshot(_ context.Context, tenantID string, takenAt time.Time) (*Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reads++
	if s.fails {
		return nil, errors.New("snapshot read: backend down")
	}
	return &Snapshot{
		TenantID: tenantID,
		Balance:  1000 - int64(s.reads)*20,
		Debited:  int64(s.reads) * 10,
		Epoch:    1,
		TakenAt:  takenAt,
	}, nil
}

func (s *driftingSource) fail() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.fails = true
}

func TestStartReconcilerReportsDriftUntilCancelled(t *testing.T) {
	t.Parallel()
	source := &driftingSource{}
	store := newMemSnapshotStore()
	rec := NewReconciler(source, []string{"t"}, store)

	var mu sync.Mutex
	var drifts []TenantDrift
	ctx, cancel := context.WithCancel(context.Background())

	StartReconciler(ctx, rec, 2*time.Millisecond, func(drift TenantDrift) {
		mu.Lock()
		drifts = append(drifts, drift)
		mu.Unlock()
	})

	// The first interval is a first sight (skipped); from the second on
	// the -10 drift must surface through the callback.
	waitUntil(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(drifts) >= 2
	}, "reconcile loop never reported the injected drift")
	mu.Lock()
	for _, drift := range drifts {
		if drift.TenantID != "t" || drift.Drift != -10 {
			mu.Unlock()
			t.Fatalf("drift = %+v, want tenant t with -10", drift)
		}
	}
	mu.Unlock()

	// A failing source must not kill the loop: reads continue past the
	// error, and no drift is invented from a failed read.
	source.fail()
	// Let any read already in flight at fail-time land, so the drift
	// count cannot change again.
	readsAtFail := source.readCount()
	waitUntil(t, func() bool { return source.readCount() >= readsAtFail+2 },
		"reconcile loop stopped after a source failure")
	driftsFrozen := len(driftsAt(&mu, &drifts))
	readsCheckpoint := source.readCount()
	waitUntil(t, func() bool { return source.readCount() >= readsCheckpoint+4 },
		"reconcile loop stopped after a source failure")
	if got := len(driftsAt(&mu, &drifts)); got != driftsFrozen {
		t.Fatalf("drifts grew from %d to %d across failed reads", driftsFrozen, got)
	}

	// Cancellation ends the loop: reads stop. A read already executing
	// at cancel-time is legal; give it room to land before sampling,
	// then confirm absence by polling across many loop periods — an
	// alive loop at this interval would keep issuing reads, so any
	// growth inside the window is a loop that ignored the cancel.
	cancel()
	time.Sleep(100 * time.Millisecond)
	before := source.readCount()
	deadline := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) {
		if after := source.readCount(); after != before {
			t.Fatalf("reconcile reads continued after cancellation: %d -> %d", before, after)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// driftsAt snapshots the drift slice under its mutex.
func driftsAt(mu *sync.Mutex, drifts *[]TenantDrift) []TenantDrift {
	mu.Lock()
	defer mu.Unlock()
	return append([]TenantDrift(nil), (*drifts)...)
}

func (s *driftingSource) readCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reads
}

// wedgedSource answers like a black-holed database: the read never
// completes on its own and returns only when its context is cut.
type wedgedSource struct {
	mu       sync.Mutex
	attempts int
}

func (s *wedgedSource) TenantSnapshot(ctx context.Context, _ string, _ time.Time) (*Snapshot, error) {
	s.mu.Lock()
	s.attempts++
	s.mu.Unlock()
	<-ctx.Done()
	return nil, ctx.Err()
}

func (s *wedgedSource) attemptCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attempts
}

// TestStartReconcilerBoundsAWedgedRound: a round stuck on a black-holed
// store is cut at the round's time budget and the loop keeps scheduling
// fresh rounds, instead of the first wedged read silencing the protocol
// for the rest of the process's life.
func TestStartReconcilerBoundsAWedgedRound(t *testing.T) {
	t.Parallel()
	source := &wedgedSource{}
	rec := NewReconciler(source, []string{"t"}, newMemSnapshotStore())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	// The interval doubles as the round budget: each wedged round is cut
	// at one interval and the next tick starts a fresh attempt.
	StartReconciler(ctx, rec, 20*time.Millisecond, nil)

	waitUntil(t, func() bool { return source.attemptCount() >= 3 },
		"reconcile loop stopped scheduling rounds after a wedged read")
	cancel()
}
