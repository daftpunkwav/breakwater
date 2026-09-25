/**
 * @file breaker_absorption_test
 * @description The structural absorption rules: expired probes are
 * reclaimed as failures by readers and reports alike, a success past
 * its own deadline is untrusted, reports from reclaimed probes are
 * absorbed, and unknown machine states fail closed.
 */
package circuit

import (
	"context"
	"testing"
	"time"
)

// openBreaker drives the machine into open, then past the cooldown into
// a half-open probe; the returned permission owns that probe.
func openThenProbe(t *testing.T, b *Registry, advance func(time.Duration)) Permission {
	t.Helper()
	ctx := context.Background()
	for range 3 {
		p, _ := b.Allow(ctx, "u")
		p.Report(OutcomeServerFault)
	}
	advance(2 * time.Second) // cooldown elapses
	probe, ok := b.Allow(ctx, "u")
	if !ok {
		t.Fatal("half-open denied the probe")
	}
	return probe
}

// TestStateOfReclaimsExpiredProbe pins the read-path reclaim: StateOf
// observing an expired probe treats it as a failure and reports open,
// so the router's pre-filter cannot see a stale half-open.
func TestStateOfReclaimsExpiredProbe(t *testing.T) {
	t.Parallel()
	b, advance := testRegistry(t, nil)
	openThenProbe(t, b, advance)

	advance(2 * time.Second) // probe deadline expires unreclaimed

	if got := b.StateOf(context.Background(), "u"); got != StateOpen {
		t.Fatalf("state = %s, want open: the expired probe must be reclaimed on read", got)
	}
}

// TestLateSuccessPastDeadlineIsUntrusted pins the deadline rule: a
// success reported after its own probe deadline is treated as a timeout
// and reopens the breaker with a fresh cooldown.
func TestLateSuccessPastDeadlineIsUntrusted(t *testing.T) {
	t.Parallel()
	b, advance := testRegistry(t, nil)
	probe := openThenProbe(t, b, advance)

	advance(2 * time.Second) // the probe overran its deadline
	probe.Report(OutcomeSuccess)

	if got := b.StateOf(context.Background(), "u"); got != StateOpen {
		t.Fatalf("state = %s, want open: a late success is unreliable", got)
	}
}

// TestReportAfterConcurrentReclaimIsAbsorbed pins the open-state
// absorption: a report from a probe whose slot was already reclaimed
// must not act on the machine.
func TestReportAfterConcurrentReclaimIsAbsorbed(t *testing.T) {
	t.Parallel()
	b, advance := testRegistry(t, nil)
	stale := openThenProbe(t, b, advance)

	// A concurrent Allow observes the expired probe and reclaims it: the
	// machine is back to open, denied while the cooldown restarts.
	advance(2 * time.Second)
	if _, ok := b.Allow(context.Background(), "u"); ok {
		t.Fatal("the reclaiming Allow must be denied")
	}

	stale.Report(OutcomeServerFault) // the very late report
	if got := b.StateOf(context.Background(), "u"); got != StateOpen {
		t.Fatalf("state = %s, want open: the stale report must be absorbed", got)
	}
}

// TestAllowFailsClosedOnUnknownState pins the fail-closed fallback: a
// machine in a state outside the vocabulary denies every call.
func TestAllowFailsClosedOnUnknownState(t *testing.T) {
	t.Parallel()
	b, _ := testRegistry(t, nil)
	b.byUpstream["u"] = &state{id: "u", name: State("corrupted")}

	if _, ok := b.Allow(context.Background(), "u"); ok {
		t.Fatal("unknown state must not be granted a call")
	}
}

// TestTransitionToSameStateIsNoOp pins the observer contract: a
// transition onto the current state neither mutates the machine nor
// fires the observer.
func TestTransitionToSameStateIsNoOp(t *testing.T) {
	t.Parallel()
	fired := false
	b, _ := testRegistry(t, nil, OnTransition(func(string, State, State) { fired = true }))

	s := &state{id: "u", name: StateClosed, failures: 2}
	b.transition(s, StateClosed)

	if fired {
		t.Fatal("observer fired for a no-op transition")
	}
	if s.failures != 2 {
		t.Fatalf("failures = %d, want untouched", s.failures)
	}
}
