/**
 * @file memory_ledger_edges_test
 * @description Edge behaviors of the in-process ledger: the injectable
 * clock, provisioning that never overwrites, and the surfaced unknown
 * lease error.
 */
package quota

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestMemoryWithClockStampsLeaseCreation(t *testing.T) {
	t.Parallel()
	m := NewMemory()
	start := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	current := start
	m.WithClock(func() time.Time { return current })
	ctx := context.Background()
	if err := m.SetBalance(ctx, "t", 100); err != nil {
		t.Fatalf("seed: %v", err)
	}

	lease, err := m.Reserve(ctx, "t", 10)
	if err != nil {
		t.Fatalf("reserve: %v", err)
	}
	if !lease.CreatedAt.Equal(start) {
		t.Fatalf("created at = %v, want the injected clock %v", lease.CreatedAt, start)
	}

	current = start.Add(time.Hour)
	if err := m.Settle(ctx, lease.ID, 10); err != nil {
		t.Fatalf("settle: %v", err)
	}
	// The terminal timestamp follows the same clock: sweeping one audit
	// window later, measured on the injected clock, purges the record.
	current = start.Add(time.Hour + leaseAuditTTL + time.Second)
	if _, err := m.SweepOnce(ctx, current, 100); err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if len(m.leases) != 0 {
		t.Fatalf("retained leases = %d, want the audit purge", len(m.leases))
	}
}

func TestMemoryEnsureBalanceNeverOverwrites(t *testing.T) {
	t.Parallel()
	m := NewMemory()
	ctx := context.Background()

	created, err := m.EnsureBalance(ctx, "t", 500)
	if err != nil || !created {
		t.Fatalf("first ensure = %v, %v; want created", created, err)
	}
	again, err := m.EnsureBalance(ctx, "t", 999)
	if err != nil || again {
		t.Fatalf("second ensure = %v, %v; want an absorbed no-op", again, err)
	}
	if bal, err := m.Balance(ctx, "t"); err != nil || bal != 500 {
		t.Fatalf("balance = %d, %v; want the original 500 untouched", bal, err)
	}
}

func TestMemoryCancelUnknownLeaseSurfaces(t *testing.T) {
	t.Parallel()
	m := NewMemory()
	err := m.Cancel(context.Background(), "no-such-lease")
	if err == nil || !strings.Contains(err.Error(), "unknown lease") {
		t.Fatalf("err = %v, want the unknown lease error surfaced", err)
	}
}
