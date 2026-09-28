/**
 * @file budget_share_test
 * @description The share-mode retry budget: the cap is a percentage of
 * the requests currently in flight, floored at a minimum, recomputed
 * at every admission; constructor arguments fail closed.
 */
package retry

import (
	"sync/atomic"
	"testing"
)

// TestShareBudgetScalesWithLiveTraffic: the admission cap tracks the
// reported in-flight count — a busy pool admits more retries than a
// quiet one at the same percentage.
func TestShareBudgetScalesWithLiveTraffic(t *testing.T) {
	t.Parallel()
	var inflight atomic.Int64
	b, err := NewShareBudget(20, 3, inflight.Load)
	if err != nil {
		t.Fatalf("NewShareBudget: %v", err)
	}

	// Quiet pool: the floor is the whole story.
	inflight.Store(10)
	if !b.Acquire() || !b.Acquire() || !b.Acquire() {
		t.Fatal("floor must admit the minimum even with no traffic")
	}
	if b.Acquire() {
		t.Fatal("admitted past the floor with 10 in flight at 20%")
	}
	b.Release()
	b.Release()
	b.Release()

	// Busy pool: 20% of 100 in flight admits 20.
	inflight.Store(100)
	for i := 0; i < 20; i++ {
		if !b.Acquire() {
			t.Fatalf("admission %d of 20 refused at 20%% of 100", i+1)
		}
	}
	if b.Acquire() {
		t.Fatal("admitted past 20% of 100")
	}
	for i := 0; i < 20; i++ {
		b.Release()
	}

	// Traffic collapses: the percentage share collapses with it and
	// the cap lands back on the floor.
	inflight.Store(1)
	for i := 0; i < 3; i++ {
		if !b.Acquire() {
			t.Fatalf("floor admission %d refused when traffic collapsed", i+1)
		}
	}
	if b.Acquire() {
		t.Fatal("admitted past the floor with 1 in flight at 20%")
	}
}

// TestShareBudgetZeroTrafficAdmitsFloor: a gateway with no other
// requests in flight still gets its minimum retry concurrency — the
// floor exists so a share of nothing is not nothing.
func TestShareBudgetZeroTrafficAdmitsFloor(t *testing.T) {
	t.Parallel()
	var inflight atomic.Int64
	b, err := NewShareBudget(50, 2, inflight.Load)
	if err != nil {
		t.Fatalf("NewShareBudget: %v", err)
	}
	if !b.Acquire() || !b.Acquire() {
		t.Fatal("floor must hold at zero traffic")
	}
	if b.Acquire() {
		t.Fatal("admitted past the floor at zero traffic")
	}
}

// TestShareBudgetShrinkDoesNotRevokeAdmittedSlots: slots admitted
// while traffic was high are never revoked when the cap shrinks — but
// the shrunken cap still binds, and the counter does not drift:
// admissions resume exactly when releases drain inFlight below it.
func TestShareBudgetShrinkDoesNotRevokeAdmittedSlots(t *testing.T) {
	t.Parallel()
	var inflight atomic.Int64
	b, err := NewShareBudget(50, 3, inflight.Load)
	if err != nil {
		t.Fatalf("NewShareBudget: %v", err)
	}

	// 50% of 100 in flight admits 50; take five.
	inflight.Store(100)
	for i := 0; i < 5; i++ {
		if !b.Acquire() {
			t.Fatalf("admission %d refused at 50%% of 100", i+1)
		}
	}

	// Traffic collapses: the cap lands on the floor while five slots
	// stay held. The over-cap holder is not revoked, and nothing
	// further fits until releases drain the counter below the floor.
	inflight.Store(1)
	if b.Acquire() {
		t.Fatal("admitted while over the shrunken cap: the cap must still bind admitted slots")
	}
	for i := 0; i < 3; i++ {
		b.Release()
	}
	// inFlight is now 2 against a floor of 3: exactly one more fits.
	if !b.Acquire() {
		t.Fatal("admission refused after releases drained inFlight below the shrunken cap")
	}
	if b.Acquire() {
		t.Fatal("admitted past the floor after the shrink")
	}
}

// TestShareBudgetRefusesBrokenArguments: a nil traffic source, an
// out-of-range percentage, and a zero floor all refuse to construct.
func TestShareBudgetRefusesBrokenArguments(t *testing.T) {
	t.Parallel()
	if _, err := NewShareBudget(20, 3, nil); err == nil {
		t.Fatal("nil in-flight source accepted")
	}
	if _, err := NewShareBudget(0, 3, func() int64 { return 0 }); err == nil {
		t.Fatal("zero percent accepted")
	}
	if _, err := NewShareBudget(101, 3, func() int64 { return 0 }); err == nil {
		t.Fatal("percent above 100 accepted")
	}
	if _, err := NewShareBudget(20, 0, func() int64 { return 0 }); err == nil {
		t.Fatal("zero floor accepted")
	}
}
