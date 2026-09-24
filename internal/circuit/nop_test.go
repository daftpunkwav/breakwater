/**
 * @file nop_test
 * @description The breaker-less composition contract: the nop grants
 * everything, never opens, and absorbs outcome reports without effect.
 */
package circuit

import (
	"context"
	"testing"
)

// TestNopBreakerGrantsEverything locks the nop's contract: every call is
// granted, outcome reports are absorbed, and the state always reads
// closed so a breaker-less deployment exposes a healthy gauge.
func TestNopBreakerGrantsEverything(t *testing.T) {
	t.Parallel()
	var b Breaker = NopBreaker{}
	ctx := context.Background()

	perm, ok := b.Allow(ctx, "u")
	if !ok {
		t.Fatal("nop breaker denied a call")
	}
	perm.Report(OutcomeServerFault)
	perm.Report(OutcomeServerFault) // double reports must be absorbable too

	if got := b.StateOf(ctx, "u"); got != StateClosed {
		t.Fatalf("state = %s, want closed: the nop never opens", got)
	}
}
