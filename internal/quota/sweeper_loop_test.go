/**
 * @file sweeper_loop_test
 * @description The background reclaim loop: it sweeps on its ticker,
 * reports reclaimed batches through the callback, keeps sweeping past a
 * backend failure and stops when its context is cancelled.
 */
package quota

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// sweepTargetStub answers the first `failures` sweeps with a backend
// error, then always reports one reclaim per sweep.
type sweepTargetStub struct {
	mu       sync.Mutex
	failures int
	calls    int
}

func (s *sweepTargetStub) SweepOnce(context.Context, time.Time, int) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	if s.calls <= s.failures {
		return 0, errors.New("ledger unavailable")
	}
	return 1, nil
}

func (s *sweepTargetStub) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// waitUntil polls cond until it holds or the deadline passes; the
// background loops under test tick every few milliseconds.
func waitUntil(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal(msg)
}

func TestStartSweeperReclaimsUntilCancelled(t *testing.T) {
	t.Parallel()
	target := &sweepTargetStub{failures: 1}
	var mu sync.Mutex
	var reclaimed []int
	ctx, cancel := context.WithCancel(context.Background())

	StartSweeper(ctx, target, 2*time.Millisecond, func(count int) {
		mu.Lock()
		reclaimed = append(reclaimed, count)
		mu.Unlock()
	})

	// The first sweep fails; the loop must survive it and keep ticking.
	waitUntil(t, func() bool { return target.callCount() >= 3 }, "sweeper stopped after a backend failure")
	waitUntil(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(reclaimed) > 0
	}, "no reclaimed batch was reported through the callback")
	mu.Lock()
	for _, count := range reclaimed {
		if count != 1 {
			mu.Unlock()
			t.Fatalf("callback reported %d reclaimed, want 1 per sweep", count)
		}
	}
	mu.Unlock()

	// Cancellation ends the loop: after a settling pause for the goroutine
	// to observe the done channel, no further sweep may follow.
	cancel()
	time.Sleep(50 * time.Millisecond)
	before := target.callCount()
	time.Sleep(80 * time.Millisecond)
	if after := target.callCount(); after != before {
		t.Fatalf("sweeps continued after cancellation: %d -> %d", before, after)
	}
}
