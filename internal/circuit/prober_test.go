/**
 * @file prober_test
 * @description Active probing: denied attempts never run the probe,
 * granted probes report through the machine, and a prober shutdown
 * mid-check counts as a client fault.
 */
package circuit

import (
	"context"
	"errors"
	"testing"
	"time"
)

// gateBreaker is a controllable Breaker for prober tests: every call
// is granted and the machine observes the reported outcomes.
type gateBreaker struct {
	granted  bool
	outcomes []Outcome
}

func (g *gateBreaker) Allow(context.Context, string) (Permission, bool) {
	if !g.granted {
		return nil, false
	}
	return &gatePermission{g}, true
}

func (g *gateBreaker) StateOf(context.Context, string) State { return StateClosed }

type gatePermission struct {
	g *gateBreaker
}

func (p *gatePermission) Report(o Outcome) { p.g.outcomes = append(p.g.outcomes, o) }

func TestActiveProbeDeniedRunsNothing(t *testing.T) {
	t.Parallel()
	b := &gateBreaker{granted: false}
	ran, err := ActiveProbe(context.Background(), b, "u1", time.Second, func(context.Context) error {
		t.Error("the probe body must not run when the breaker denies the attempt")
		return nil
	})
	if ran || err != nil {
		t.Fatalf("ran = %v, err = %v; a denied breaker must not run the probe", ran, err)
	}
	if len(b.outcomes) != 0 {
		t.Fatalf("outcomes = %v, want none", b.outcomes)
	}
}

func TestActiveProbeReportsOutcome(t *testing.T) {
	t.Parallel()
	b := &gateBreaker{granted: true}
	ran, err := ActiveProbe(context.Background(), b, "u1", 0, func(context.Context) error { return nil })
	if !ran || err != nil {
		t.Fatalf("ran = %v, err = %v; want the probe run and healthy", ran, err)
	}
	if len(b.outcomes) != 1 || b.outcomes[0] != OutcomeSuccess {
		t.Fatalf("outcomes = %v, want one success", b.outcomes)
	}

	ran, err = ActiveProbe(context.Background(), b, "u1", 0, func(context.Context) error {
		return errors.New("probe status 503")
	})
	if !ran || err == nil {
		t.Fatalf("ran = %v, err = %v; want the failed probe surfaced", ran, err)
	}
	if b.outcomes[1] != OutcomeServerFault {
		t.Fatalf("outcome = %v, want a server fault for a failed probe", b.outcomes[1])
	}
}

func TestActiveProbeTimesOut(t *testing.T) {
	t.Parallel()
	b := &gateBreaker{granted: true}
	ran, err := ActiveProbe(context.Background(), b, "u1", 10*time.Millisecond, func(ctx context.Context) error {
		select {
		case <-time.After(time.Second):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	if !ran || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("ran = %v, err = %v; want the probe bounded by the timeout", ran, err)
	}
	if b.outcomes[0] != OutcomeServerFault {
		t.Fatalf("outcome = %v, want a server fault for a timed-out probe", b.outcomes[0])
	}
}

// TestActiveProbeShutdownIsClientFault: when the prober's own context
// is cancelled mid-check, the verdict is a client fault — a shutdown
// must not advance any failure counter.
func TestActiveProbeShutdownIsClientFault(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	b := &gateBreaker{granted: true}
	ran, err := ActiveProbe(ctx, b, "u1", 0, func(context.Context) error {
		cancel()
		return context.Canceled
	})
	if !ran || !errors.Is(err, context.Canceled) {
		t.Fatalf("ran = %v, err = %v; want the cancellation surfaced", ran, err)
	}
	if b.outcomes[0] != OutcomeClientFault {
		t.Fatalf("outcome = %v, want a client fault for the prober's own shutdown", b.outcomes[0])
	}
}

// TestActiveProbeDrivesRecovery: the integration story — an open
// breaker rejects probes until its cooldown elapses, then one active
// probe closes it without any real traffic.
func TestActiveProbeDrivesRecovery(t *testing.T) {
	t.Parallel()
	now := time.Now()
	b := NewRegistry(Config{FailThreshold: 1, Cooldown: time.Minute}, WithClock(func() time.Time { return now }))

	// Force the breaker open.
	perm, ok := b.Allow(context.Background(), "u1")
	if !ok {
		t.Fatal("closed breaker denied the first call")
	}
	perm.Report(OutcomeServerFault)
	if b.StateOf(context.Background(), "u1") != StateOpen {
		t.Fatal("breaker did not open")
	}

	// Cooldown outstanding: the probe is denied outright.
	ran, err := ActiveProbe(context.Background(), b, "u1", time.Second, func(context.Context) error { return nil })
	if ran || err != nil {
		t.Fatalf("probe ran during cooldown: ran = %v, err = %v", ran, err)
	}

	// Cooldown elapsed: the probe becomes the half-open probe and its
	// success closes the breaker.
	now = now.Add(2 * time.Minute)
	ran, err = ActiveProbe(context.Background(), b, "u1", time.Second, func(context.Context) error { return nil })
	if !ran || err != nil {
		t.Fatalf("probe after cooldown: ran = %v, err = %v", ran, err)
	}
	if state := b.StateOf(context.Background(), "u1"); state != StateClosed {
		t.Fatalf("state = %v, want closed after a healthy active probe", state)
	}
}
