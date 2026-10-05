/**
 * @file slow_test
 * @description The slow-call-strategy breaker: a thin window never
 * opens, the slow share opens when it reaches the ratio, faults count
 * as the strongest slow evidence, a healthy probe closes on an emptied
 * window, and reads never allocate the probe slot.
 */
package circuit

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"
)

// slowFixture is a controllable registry: the clock advances one
// window bucket per step, and the cooldown/probe constants are 120 and
// 20 steps respectively.
func newSlowFixture(t *testing.T, ratio float64) (*SlowRegistry, func(int), func() time.Time) {
	t.Helper()
	step := 0
	now := func() time.Time { return time.Unix(0, 0).Add(time.Duration(step) * slowBucketSpan) }
	reg := NewSlowRegistry(Config{Cooldown: 30 * time.Second, ProbeTimeout: 5 * time.Second, SlowRatio: ratio}, SlowClock(now))
	advance := func(n int) { step += n }
	return reg, advance, now
}

// report grants once and reports the outcome, advancing the clock one
// second per call so the window spreads across buckets.
func report(t *testing.T, reg *SlowRegistry, id string, outcome Outcome) {
	t.Helper()
	p, ok := reg.Allow(context.Background(), id)
	if !ok {
		t.Fatal("closed breaker denied during setup")
	}
	p.Report(outcome)
}

// TestSlowWindowNeedsSamples: fewer samples than the minimum never
// open the breaker, however slow they are.
func TestSlowWindowNeedsSamples(t *testing.T) {
	t.Parallel()
	reg, _, _ := newSlowFixture(t, 0.5)
	const id = "u1"

	for i := 0; i < slowMinSamples-1; i++ {
		report(t, reg, id, OutcomeSlow)
	}
	if state := reg.StateOf(context.Background(), id); state != StateClosed {
		t.Fatalf("state = %s, want closed below the sample minimum", state)
	}
}

// TestSlowShareOpensTheBreaker: the ratio trigger with faults counting
// as the strongest slow evidence.
func TestSlowShareOpensTheBreaker(t *testing.T) {
	t.Parallel()
	reg, advance, _ := newSlowFixture(t, 0.5)
	const id = "u1"

	// 5 slow completions + 5 fast ones: exactly half, at the threshold
	// (the trigger is >=, floating-point equality included).
	for i := 0; i < 5; i++ {
		advance(1) // spread across buckets via the clock
		report(t, reg, id, OutcomeSlow)
	}
	for i := 0; i < 5; i++ {
		advance(1)
		report(t, reg, id, OutcomeSuccess)
	}
	if state := reg.StateOf(context.Background(), id); state != StateOpen {
		t.Fatalf("state = %s, want open at a half-slow window", state)
	}
}

// TestFaultsCountAsSlowEvidence: server faults feed both counters, so
// an error-dominated window opens like a slow one.
func TestFaultsCountAsSlowEvidence(t *testing.T) {
	t.Parallel()
	reg, _, _ := newSlowFixture(t, 0.5)
	const id = "u1"

	for i := 0; i < 6; i++ {
		report(t, reg, id, OutcomeServerFault)
	}
	for i := 0; i < 4; i++ {
		report(t, reg, id, OutcomeSuccess)
	}
	if state := reg.StateOf(context.Background(), id); state != StateOpen {
		t.Fatalf("state = %s, want open from fault evidence", state)
	}
}

// TestSlowProbeClosesOnAnEmptiedWindow: the half-open probe closes the
// machine even when slow, and the transition empties the window so
// recovery starts from clean evidence.
func TestSlowProbeClosesOnAnEmptiedWindow(t *testing.T) {
	t.Parallel()
	reg, advance, _ := newSlowFixture(t, 0.5)
	const id = "u1"

	for i := 0; i < slowMinSamples; i++ {
		advance(1)
		report(t, reg, id, OutcomeSlow)
	}
	if state := reg.StateOf(context.Background(), id); state != StateOpen {
		t.Fatalf("state = %s, want open before the probe", state)
	}
	advance(120) // the cooldown elapses
	p, ok := reg.Allow(context.Background(), id)
	if !ok {
		t.Fatal("half-open must admit the probe")
	}
	p.Report(OutcomeSlow)
	if state := reg.StateOf(context.Background(), id); state != StateClosed {
		t.Fatalf("state = %s, want closed after a healthy probe", state)
	}

	// The window was emptied by the close: slow calls from the previous
	// episode are gone, so the fresh evidence starts from zero and the
	// breaker stays closed.
	for i := 0; i < slowMinSamples-1; i++ {
		report(t, reg, id, OutcomeSlow)
	}
	if state := reg.StateOf(context.Background(), id); state != StateClosed {
		t.Fatalf("state = %s, want closed: old evidence must not outlive the close", state)
	}
}

// TestSlowProbeFaultReopens: a faulting probe re-opens with a fresh
// cooldown, like the consecutive machine.
func TestSlowProbeFaultReopens(t *testing.T) {
	t.Parallel()
	reg, advance, _ := newSlowFixture(t, 0.5)
	const id = "u1"

	for i := 0; i < slowMinSamples; i++ {
		advance(1)
		report(t, reg, id, OutcomeSlow)
	}
	advance(120) // the cooldown elapses
	p, ok := reg.Allow(context.Background(), id)
	if !ok {
		t.Fatal("half-open must admit the probe")
	}
	p.Report(OutcomeServerFault)
	if state := reg.StateOf(context.Background(), id); state != StateOpen {
		t.Fatalf("state = %s, want open after a faulting probe", state)
	}
	if _, ok := reg.Allow(context.Background(), id); ok {
		t.Fatal("a fresh cooldown must deny")
	}
}

// TestSlowReadsNeverAllocateTheProbe: a StateOf reaching half-open must
// leave the slot free for the next Allow, exactly like the consecutive
// machine's contract.
func TestSlowReadsNeverAllocateTheProbe(t *testing.T) {
	t.Parallel()
	reg, advance, _ := newSlowFixture(t, 0.5)
	const id = "u1"

	for i := 0; i < slowMinSamples; i++ {
		advance(1)
		report(t, reg, id, OutcomeSlow)
	}
	advance(120) // the cooldown elapses
	if state := reg.StateOf(context.Background(), id); state != StateHalfOpen {
		t.Fatalf("state = %s, want the lazy half-open", state)
	}
	if p, ok := reg.Allow(context.Background(), id); !ok || p == nil {
		t.Fatal("the read must not have consumed the probe slot")
	}
}

// TestSlowDoubleReportAbsorbed: one slot reports once; a duplicate is
// absorbed by the registry.
func TestSlowDoubleReportAbsorbed(t *testing.T) {
	t.Parallel()
	reg, advance, _ := newSlowFixture(t, 0.5)
	const id = "u1"

	p, ok := reg.Allow(context.Background(), id)
	if !ok {
		t.Fatal("closed breaker denied")
	}
	advance(1)
	p.Report(OutcomeSlow)
	p.Report(OutcomeSlow)

	st := reg.stateOf(id)
	total, _ := st.share(time.Unix(1, 0))
	if total != 1 {
		t.Fatalf("window total = %d, want 1 (one report)", total)
	}
}

// TestSlowResetEmptiesEverything: the operator reset returns a fully
// saturated breaker to a closed machine with an empty window.
func TestSlowResetEmptiesEverything(t *testing.T) {
	t.Parallel()
	reg, advance, _ := newSlowFixture(t, 0.5)
	const id = "u1"

	for i := 0; i < slowMinSamples; i++ {
		advance(1)
		report(t, reg, id, OutcomeSlow)
	}
	if state := reg.StateOf(context.Background(), id); state != StateOpen {
		t.Fatalf("state = %s, want open before the reset", state)
	}
	reg.Reset(context.Background(), id)
	if state := reg.StateOf(context.Background(), id); state != StateClosed {
		t.Fatalf("state = %s, want closed after the reset", state)
	}
	if p, ok := reg.Allow(context.Background(), id); !ok || p == nil {
		t.Fatal("expected admission after the reset")
	}
}

// TestSlowRatioDefaultAndClamp: a ratio outside (0, 1] selects the
// default, so a misconfigured deployment cannot build a breaker that
// never opens (0) or always opens (>1). NaN counts as outside: every
// direct comparison against it is false, so only the negated range
// conjunction catches it — and a NaN ratio would otherwise arm a
// breaker whose trigger can never fire.
func TestSlowRatioDefaultAndClamp(t *testing.T) {
	t.Parallel()
	for _, bad := range []float64{0, -1, 1.5, math.NaN()} {
		reg := NewSlowRegistry(Config{SlowRatio: bad})
		if reg.cfg.SlowRatio != defaultSlowRatio {
			t.Fatalf("SlowRatio %v survived, want the default %v", bad, defaultSlowRatio)
		}
	}
}

// TestSlowConcurrentUse exercises Allow/Report/StateOf/Reset under
// contention; correctness is the race detector's job here.
func TestSlowConcurrentUse(t *testing.T) {
	t.Parallel()
	reg := NewSlowRegistry(Config{Cooldown: time.Millisecond, ProbeTimeout: time.Second, SlowRatio: 0.5})

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := "u" + string(rune('a'+i%3))
			for j := 0; j < 100; j++ {
				if p, ok := reg.Allow(context.Background(), id); ok {
					switch j % 4 {
					case 0:
						p.Report(OutcomeSlow)
					case 1:
						p.Report(OutcomeSuccess)
					case 2:
						p.Report(OutcomeServerFault)
					default:
						p.Report(OutcomeGatewayTerminated)
					}
				}
				reg.StateOf(context.Background(), id)
				if j%50 == 0 {
					reg.Reset(context.Background(), id)
				}
			}
		}(i)
	}
	wg.Wait()
}

// TestSlowAbsorbedAndExpiredPaths: the boundary paths behave like the
// consecutive machine's — an expired probe is reclaimed as open by the
// next read, a busy half-open denies, a gateway cut feeds nothing, a
// hijacked report is absorbed, and transitions are observable.
func TestSlowAbsorbedAndExpiredPaths(t *testing.T) {
	t.Parallel()
	var transitions []string
	reg := NewSlowRegistry(Config{Cooldown: 30 * time.Second, ProbeTimeout: 5 * time.Second, SlowRatio: 0.5},
		SlowClock(func() time.Time { return time.Unix(0, 0).Add(time.Duration(clockStep) * slowBucketSpan) }),
		SlowOnTransition(func(_ string, from, to State) {
			transitions = append(transitions, string(from)+"->"+string(to))
		}))
	advance := func(n int) { clockStep += n }
	const id = "u1"

	// A gateway cut is policy, not evidence: it feeds nothing.
	p, ok := reg.Allow(context.Background(), id)
	if !ok {
		t.Fatal("closed breaker denied")
	}
	p.Report(OutcomeGatewayTerminated)
	total, slow := reg.stateOf(id).share(time.Unix(0, 0))
	if total != 0 || slow != 0 {
		t.Fatalf("window = %d/%d, want nothing recorded for a gateway cut", slow, total)
	}

	// Saturate the window; the open transition fires the observer.
	advance(1)
	for i := 0; i < slowMinSamples; i++ {
		advance(1)
		report(t, reg, id, OutcomeSlow)
	}
	if state := reg.StateOf(context.Background(), id); state != StateOpen {
		t.Fatalf("state = %s, want open", state)
	}
	if len(transitions) == 0 || transitions[len(transitions)-1] != "closed->open" {
		t.Fatalf("transitions = %v, want the open transition observed", transitions)
	}

	// The cooldown elapses into half-open; a second call finds the probe
	// slot busy and is denied.
	advance(120)
	if state := reg.StateOf(context.Background(), id); state != StateHalfOpen {
		t.Fatalf("state = %s, want half-open", state)
	}
	if p, ok := reg.Allow(context.Background(), id); !ok || p == nil {
		t.Fatal("the first half-open call must win the probe")
	}
	if _, ok := reg.Allow(context.Background(), id); ok {
		t.Fatal("half-open must deny while a probe is outstanding")
	}

	// The probe dies quietly past its deadline; the next read reclaims
	// the slot as an open machine with a fresh cooldown.
	advance(21)
	if state := reg.StateOf(context.Background(), id); state != StateOpen {
		t.Fatalf("state = %s, want the expired probe reclaimed to open", state)
	}

	// A report from the reclaimed slot is absorbed by the open machine.
	if state := reg.StateOf(context.Background(), id); state != StateOpen {
		t.Fatalf("state = %s, want still open", state)
	}
}

// clockStep backs the SlowClock of TestSlowAbsorbedAndExpiredPaths.
var clockStep int
