/**
 * @file ledger_test_helpers
 * @description Shared test scaffolding for the ledger backends: seeded
 * ledgers and the concurrent-drain reconciliation evidence both
 * implementations must hold.
 */
package quota

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/alicebob/miniredis/v2"
)

// newSeededMemory returns an in-memory ledger provisioned with the
// tenant balance under the shared test tenant id "t".
func newSeededMemory(t *testing.T, balance int64) (*Memory, context.Context) {
	t.Helper()
	m := NewMemory()
	ctx := context.Background()
	if err := m.SetBalance(context.Background(), "t", balance); err != nil {
		t.Fatalf("seed: %v", err)
	}
	return m, ctx
}

// newSeededRedisLedger returns a miniredis-backed ledger provisioned
// with the tenant balance under the shared test tenant id "t",
// together with the store handle the key-level assertions read.
func newSeededRedisLedger(t *testing.T, balance int64) (*Redis, *miniredis.Miniredis, context.Context) {
	t.Helper()
	r, mr := newTestLedger(t)
	ctx := context.Background()
	if err := r.SetBalance(ctx, "t", balance); err != nil {
		t.Fatalf("seed: %v", err)
	}
	return r, mr, ctx
}

// runConcurrentDrain is the reconciliation evidence shared by the
// memory and Redis backends: many workers reserve and settle
// concurrently against a shared balance, each round consuming a
// deterministic slice of the reservation. It returns the consumed
// total and the final balance; the caller asserts the identity
// initial = final + consumed. Run under -race.
func runConcurrentDrain(t *testing.T, l Ledger, initial, reserveAmt int64) (int64, int64) {
	t.Helper()
	const (
		workers = 64
		rounds  = 25
	)
	ctx := context.Background()
	if err := l.SetBalance(ctx, "t", initial); err != nil {
		t.Fatalf("seed: %v", err)
	}

	var consumed atomic.Int64
	var wg sync.WaitGroup
	errs := make(chan error, workers*rounds)
	for w := range workers {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for round := range rounds {
				lease, err := l.Reserve(ctx, "t", reserveAmt)
				if err != nil {
					errs <- err
					continue
				}
				// Vary usage deterministically across the refund range.
				used := int64((worker + round) % 101)
				if err := l.Settle(ctx, lease.ID, used); err != nil {
					errs <- err
					continue
				}
				consumed.Add(used)
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent drain error: %v", err)
	}

	finalBalance, err := l.Balance(ctx, "t")
	if err != nil {
		t.Fatalf("balance: %v", err)
	}
	return consumed.Load(), finalBalance
}
