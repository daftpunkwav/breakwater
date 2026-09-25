/**
 * @file memory_edge_test
 * @description Edge branches of the in-memory bucket math: the deficit
 * wait guards and the refund no-ops, which the semantics tests above
 * only touch incidentally.
 */
package limiter

import (
	"context"
	"testing"
	"time"
)

func TestDeficitWaitGuards(t *testing.T) {
	t.Parallel()
	// A zero capacity or a zero deficit needs no wait.
	if got := deficitWait(0, 100); got != 0 {
		t.Fatalf("zero deficit wait = %v, want 0", got)
	}
	if got := deficitWait(5, 0); got != 0 {
		t.Fatalf("disabled ceiling wait = %v, want 0", got)
	}
}

func TestDeficitWaitClampsToMillisecond(t *testing.T) {
	t.Parallel()
	// 6000 TPM = 100 tokens/s: a 0.001-token deficit is 10µs of refill,
	// clamped up to the minimum schedulable wait.
	if got := deficitWait(0.001, 6000); got != time.Millisecond {
		t.Fatalf("sub-millisecond wait = %v, want the 1ms clamp", got)
	}
}

func TestMemoryRefundNoOps(t *testing.T) {
	t.Parallel()
	m := NewMemory()
	ctx := context.Background()
	limits := Limits{RPM: 0, TPM: 100}

	// A refund of nothing, and a refund against a disabled TPM ceiling,
	// are both no-ops that must not touch any bucket.
	if err := m.Refund(ctx, "t", limits, 0); err != nil {
		t.Fatalf("zero refund: %v", err)
	}
	if err := m.Refund(ctx, "t", Limits{RPM: 10, TPM: 0}, 50); err != nil {
		t.Fatalf("refund with disabled TPM: %v", err)
	}
	if d, err := m.Allow(ctx, "t", limits, 100); err != nil || !d.Allowed {
		t.Fatalf("buckets after no-op refunds: %v %v", d.Allowed, err)
	}
}
