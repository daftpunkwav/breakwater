/**
 * @file breaker_defaults_test
 * @description Configuration and lookup defaults: a zero Config selects
 * the documented defaults, and an unknown upstream reads closed.
 */
package circuit

import (
	"context"
	"testing"
	"time"
)

// TestZeroConfigSelectsDefaults pins the documented defaults: with no
// configuration at all, five consecutive server faults open the breaker
// — fewer do not.
func TestZeroConfigSelectsDefaults(t *testing.T) {
	t.Parallel()
	b := NewRegistry(Config{})
	ctx := context.Background()

	for i := 0; i < 4; i++ {
		perm, ok := b.Allow(ctx, "u")
		if !ok {
			t.Fatalf("call %d denied while closed", i)
		}
		perm.Report(OutcomeServerFault)
	}
	if got := b.StateOf(ctx, "u"); got != StateClosed {
		t.Fatalf("state after 4 faults = %s, want closed (default threshold is 5)", got)
	}

	perm, ok := b.Allow(ctx, "u")
	if !ok {
		t.Fatal("fifth call denied while closed")
	}
	perm.Report(OutcomeServerFault)
	if got := b.StateOf(ctx, "u"); got != StateOpen {
		t.Fatalf("state after 5 faults = %s, want open", got)
	}
}

// TestStateOfUnknownUpstreamIsClosed pins the lazy creation rule: the
// first read of an unseen upstream reports closed and never panics.
func TestStateOfUnknownUpstreamIsClosed(t *testing.T) {
	t.Parallel()
	b := NewRegistry(Config{FailThreshold: 1, Cooldown: time.Hour})
	if got := b.StateOf(context.Background(), "never-seen"); got != StateClosed {
		t.Fatalf("state = %s, want closed", got)
	}
}
