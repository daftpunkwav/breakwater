/**
 * @file breaker_reset_test
 * @description The operator reset: forcing a machine back to closed
 * from every state, with the failure count and any outstanding probe
 * cleared.
 */
package circuit

import (
	"context"
	"testing"
	"time"
)

func TestResetOpensToClosed(t *testing.T) {
	t.Parallel()
	b := NewRegistry(Config{FailThreshold: 1})
	perm, ok := b.Allow(context.Background(), "u1")
	if !ok {
		t.Fatal("closed breaker denied the setup call")
	}
	perm.Report(OutcomeServerFault)
	if b.StateOf(context.Background(), "u1") != StateOpen {
		t.Fatal("setup: breaker did not open")
	}

	b.Reset(context.Background(), "u1")
	if state := b.StateOf(context.Background(), "u1"); state != StateClosed {
		t.Fatalf("state = %v, want closed after the reset", state)
	}
	// The call slot is granted again without waiting out a cooldown.
	if p, ok := b.Allow(context.Background(), "u1"); !ok {
		t.Fatal("reset breaker denied a call")
	} else {
		p.Report(OutcomeSuccess)
	}
}

func TestResetClearsTheFailureCount(t *testing.T) {
	t.Parallel()
	b := NewRegistry(Config{FailThreshold: 3})
	for i := 0; i < 2; i++ {
		perm, ok := b.Allow(context.Background(), "u1")
		if !ok {
			t.Fatalf("call %d denied on a closed breaker", i)
		}
		perm.Report(OutcomeServerFault)
	}

	// Two failures of three: the reset wipes them, so one further
	// failure must not open the breaker.
	b.Reset(context.Background(), "u1")
	perm, ok := b.Allow(context.Background(), "u1")
	if !ok {
		t.Fatal("reset breaker denied a call")
	}
	perm.Report(OutcomeServerFault)
	if state := b.StateOf(context.Background(), "u1"); state != StateClosed {
		t.Fatalf("state = %v, want closed: the reset cleared the failure count", state)
	}
}

func TestResetClearsAnOutstandingProbe(t *testing.T) {
	t.Parallel()
	now := time.Now()
	b := NewRegistry(Config{FailThreshold: 2, Cooldown: time.Minute}, WithClock(func() time.Time { return now }))

	// Force the breaker open with two consecutive faults.
	for i := 0; i < 2; i++ {
		perm, ok := b.Allow(context.Background(), "u1")
		if !ok {
			t.Fatalf("setup: closed breaker denied call %d", i)
		}
		perm.Report(OutcomeServerFault)
	}
	if b.StateOf(context.Background(), "u1") != StateOpen {
		t.Fatal("setup: breaker did not open")
	}

	// Elapse the cooldown and take the half-open probe slot.
	now = now.Add(2 * time.Minute)
	probe, ok := b.Allow(context.Background(), "u1")
	if !ok {
		t.Fatal("setup: cooldown-elapsed breaker denied the probe")
	}
	if state := b.StateOf(context.Background(), "u1"); state != StateHalfOpen {
		t.Fatalf("setup state = %v, want half-open", state)
	}

	b.Reset(context.Background(), "u1")
	if state := b.StateOf(context.Background(), "u1"); state != StateClosed {
		t.Fatalf("state = %v, want closed with the probe cleared", state)
	}
	// The abandoned probe's late report lands on the fresh machine as
	// an ordinary outcome — it counts once toward the failure count, it
	// does not resurrect the cleared probe or corrupt the state.
	probe.Report(OutcomeServerFault)
	if state := b.StateOf(context.Background(), "u1"); state != StateClosed {
		t.Fatalf("state = %v, want closed: one late fault on a fresh machine is under threshold", state)
	}
	// The machine keeps admitting calls.
	if p, ok := b.Allow(context.Background(), "u1"); !ok {
		t.Fatal("reset breaker denied a call")
	} else {
		p.Report(OutcomeSuccess)
	}
	if state := b.StateOf(context.Background(), "u1"); state != StateClosed {
		t.Fatalf("state = %v, want closed", state)
	}
}

func TestResetIsANoOpOnClosed(t *testing.T) {
	t.Parallel()
	b := NewRegistry(Config{FailThreshold: 5})
	b.Reset(context.Background(), "never-touched")
	if state := b.StateOf(context.Background(), "never-touched"); state != StateClosed {
		t.Fatalf("state = %v, want closed", state)
	}
}
