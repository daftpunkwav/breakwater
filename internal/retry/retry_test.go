/**
 * @file retry_test
 * @description Budget concurrency tests: the in-flight cap must hold
 * under contention and unbalanced releases must stay absorbed (I5).
 */
package retry

import (
	"sync"
	"testing"
)

func TestBudgetCapsInFlightRetries(t *testing.T) {
	t.Parallel()
	b := NewBudget(3)

	acquired := make(chan struct{}, 8)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if b.Acquire() {
				acquired <- struct{}{}
			}
		}()
	}
	wg.Wait()

	if got := len(acquired); got != 3 {
		t.Fatalf("acquired = %d, want 3", got)
	}
}

func TestBudgetNegativeCapAdmitsNothing(t *testing.T) {
	t.Parallel()
	b := NewBudget(-5)
	if b.Acquire() {
		t.Fatal("a negative cap must clamp to a budget that admits nothing")
	}
}

func TestBudgetReleaseAbsorbsImbalance(t *testing.T) {
	t.Parallel()
	b := NewBudget(2)

	for i := 0; i < 2; i++ {
		if !b.Acquire() {
			t.Fatal("acquire within cap failed")
		}
	}
	if b.Acquire() {
		t.Fatal("acquire beyond cap succeeded")
	}
	for i := 0; i < 5; i++ {
		b.Release() // over-release must not corrupt the counter
	}
	if !b.Acquire() {
		t.Fatal("budget did not recover after releases")
	}
}
