/**
 * @file ratio_test
 * @description The ratio-strategy breaker: tiny windows are protected,
 * the deny probability tracks the failure share, healthy history and
 * trailing successes relax it, the forced pass keeps recovery
 * observable, and the window forgets on expiry and on Reset.
 */
package circuit

import (
	"context"
	"sync"
	"testing"
	"time"
)

// ratioClock is a controllable clock for the ratio tests.
type ratioClock struct{ t time.Time }

func (c *ratioClock) Now() time.Time          { return c.t }
func (c *ratioClock) Advance(d time.Duration) { c.t = c.t.Add(d) }

// ratioRandom is a switchable uniform source: it always draws the
// value last set, so denial outcomes stay deterministic.
type ratioRandom struct{ v float64 }

func (r *ratioRandom) Float64() float64 { return r.v }

// newRatioFixture builds a registry over the controllable clock and
// random source, plus a convenience Allow.
func newRatioFixture(t *testing.T) (*RatioRegistry, *ratioClock, *ratioRandom) {
	t.Helper()
	clock := &ratioClock{t: time.Unix(0, 0)}
	draw := &ratioRandom{v: 1}
	reg := NewRatioRegistry(
		RatioClock(clock.Now),
		RatioRandomSource(draw.Float64),
	)
	return reg, clock, draw
}

// feed records n server faults in the current bucket by granting
// (the draw is high enough to never deny) and reporting.
func feed(t *testing.T, reg *RatioRegistry, id string, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		p, ok := reg.Allow(context.Background(), id)
		if !ok {
			t.Fatalf("setup grant %d/%d was denied", i+1, n)
		}
		p.Report(OutcomeServerFault)
	}
}

// TestRatioProtectionAdmitsTinyWindows: five or fewer events can never
// deny, and the sixth failure switches the guard on.
func TestRatioProtectionAdmitsTinyWindows(t *testing.T) {
	t.Parallel()
	reg, _, draw := newRatioFixture(t)
	const id = "u1"

	feed(t, reg, id, 5)
	draw.v = 0
	if p, ok := reg.Allow(context.Background(), id); !ok || p == nil {
		t.Fatal("guard denied a five-event window")
	}

	// One more failure lifts the window past the protection floor.
	draw.v = 1
	feed(t, reg, id, 1)
	draw.v = 0
	if p, ok := reg.Allow(context.Background(), id); ok || p != nil {
		t.Fatal("guard admitted a window whose deny ratio is positive at draw 0")
	}
}

// TestRatioDenialTracksFailureShare: a failure-dominated window denies
// low draws and admits high ones, with the failure-only streak
// discounting past accepts.
func TestRatioDenialTracksFailureShare(t *testing.T) {
	t.Parallel()
	reg, _, draw := newRatioFixture(t)
	const id = "u1"

	feed(t, reg, id, 20)

	draw.v = 0
	if p, ok := reg.Allow(context.Background(), id); ok {
		_ = p
		t.Fatal("expected a denial at draw 0")
	}
	draw.v = 0.8
	if p, ok := reg.Allow(context.Background(), id); !ok || p == nil {
		t.Fatal("expected admission at draw 0.8")
	}
}

// TestRatioWorkingBucketsDilute: trailing healthy buckets shrink the
// deny ratio — a bad bucket inside a clean window must deny less, not
// more.
func TestRatioWorkingBucketsDilute(t *testing.T) {
	t.Parallel()
	reg, clock, draw := newRatioFixture(t)
	const id = "u1"

	feed(t, reg, id, 10)
	// The successes must land in their own buckets: a bucket holding a
	// failure is never a working bucket, no matter its successes.
	for i := 0; i < 2; i++ {
		clock.Advance(ratioBucketSpan)
		p, ok := reg.Allow(context.Background(), id)
		if !ok {
			t.Fatalf("setup success %d was denied", i)
		}
		p.Report(OutcomeSuccess)
	}

	// Without dilution the ratio is 4/13 ≈ 0.308; with two working
	// buckets it is ≈ 0.292, so a 0.3 draw passes only because the
	// healthy buckets diluted it.
	draw.v = 0.3
	if p, ok := reg.Allow(context.Background(), id); !ok || p == nil {
		t.Fatal("expected the working buckets to dilute the ratio below 0.3")
	}
}

// TestRatioForcePassAdmitsOnePerInterval: while the guard denies, one
// call per forced-pass interval gets through; a denial never refreshes
// the cursor.
func TestRatioForcePassAdmitsOnePerInterval(t *testing.T) {
	t.Parallel()
	reg, clock, draw := newRatioFixture(t)
	const id = "u1"

	feed(t, reg, id, 10)
	draw.v = 0

	if p, ok := reg.Allow(context.Background(), id); ok {
		_ = p
		t.Fatal("expected a denial before any forced pass")
	}

	clock.Advance(ratioForcePass / 2)
	if p, ok := reg.Allow(context.Background(), id); ok {
		_ = p
		t.Fatal("expected a denial within the forced-pass interval")
	}

	clock.Advance(ratioForcePass / 2)
	p, ok := reg.Allow(context.Background(), id)
	if !ok || p == nil {
		t.Fatal("expected the forced pass to admit one call")
	}
	// The probe's outcome is failure evidence like any other; the
	// pressure stays on.
	p.Report(OutcomeServerFault)

	clock.Advance(9 * ratioForcePass / 10)
	if p, ok := reg.Allow(context.Background(), id); ok {
		_ = p
		t.Fatal("expected the forced pass to admit at most one call per interval")
	}

	clock.Advance(ratioForcePass / 5)
	if p, ok := reg.Allow(context.Background(), id); !ok || p == nil {
		t.Fatal("expected the next forced pass after a full interval")
	}
}

// TestRatioSuccessesRelaxTheGuard: enough healthy answers drive the
// deny ratio to zero even after a saturated window.
func TestRatioSuccessesRelaxTheGuard(t *testing.T) {
	t.Parallel()
	reg, _, draw := newRatioFixture(t)
	const id = "u1"

	feed(t, reg, id, 20)
	draw.v = 0
	if p, ok := reg.Allow(context.Background(), id); ok {
		_ = p
		t.Fatal("expected a denial before recovery")
	}

	// The window holds 20 faults plus the injected denial from the
	// check above. With the accepts weight at 1.5, the numerator
	// (total − 5 − 1.5·accepts) reaches zero exactly at 32 successes.
	draw.v = 1
	for i := 0; i < 32; i++ {
		p, ok := reg.Allow(context.Background(), id)
		if !ok {
			t.Fatalf("recovery grant %d was denied", i)
		}
		p.Report(OutcomeSuccess)
	}
	draw.v = 0
	if p, ok := reg.Allow(context.Background(), id); !ok || p == nil {
		t.Fatal("expected full admission after sustained successes")
	}
}

// TestRatioWindowExpiry: a window with no recent events admits
// everything — idle time carries no grudge.
func TestRatioWindowExpiry(t *testing.T) {
	t.Parallel()
	reg, clock, draw := newRatioFixture(t)
	const id = "u1"

	feed(t, reg, id, 20)
	clock.Advance(10*ratioBucketSpan*ratioBuckets + ratioBucketSpan)
	draw.v = 0
	if p, ok := reg.Allow(context.Background(), id); !ok || p == nil {
		t.Fatal("expected a fully expired window to admit")
	}
}

// TestRatioHistorySkipsExpiredWithoutResetting: reads see span-skipped
// history; only a write rolls the ring.
func TestRatioHistorySkipsExpiredWithoutResetting(t *testing.T) {
	t.Parallel()
	reg, clock, _ := newRatioFixture(t)
	const id = "u1"
	now := clock.Now()

	s := reg.stateOf(id, now)
	s.add(now, 0, 3, 0)
	later := now.Add(ratioBucketSpan)
	s.add(later, 2, 0, 0)

	// Two buckets written; at +8s the span is 32, so eight buckets are
	// visible and both writes survive.
	h := s.history(now.Add(8 * time.Second))
	if h.total != 5 || h.accepts != 2 {
		t.Fatalf("history = %+v, want total 5 accepts 2", h)
	}

	// Past the whole window the same read sees nothing, and the
	// buckets it skipped are still intact underneath.
	empty := s.history(now.Add(10*time.Second + ratioBucketSpan))
	if empty.total != 0 {
		t.Fatalf("expired history = %+v, want empty", empty)
	}
	if s.buckets[0].failure != 3 || s.buckets[1].success != 2 {
		t.Fatal("a read must not roll the ring")
	}
}

// TestRatioRollResetsAndAligns: a write after a gap resets the buckets
// it passes, lands in the new current bucket, and keeps lastWrite on a
// bucket boundary.
func TestRatioRollResetsAndAligns(t *testing.T) {
	t.Parallel()
	reg, clock, _ := newRatioFixture(t)
	const id = "u1"
	now := clock.Now()

	s := reg.stateOf(id, now)
	s.add(now, 1, 0, 0)
	s.add(now.Add(ratioBucketSpan), 0, 2, 0)

	rolled := now.Add(3*ratioBucketSpan + 100*time.Millisecond)
	s.add(rolled, 0, 0, 1)

	if s.offset != 3 {
		t.Fatalf("offset = %d, want 3", s.offset)
	}
	if want := now.Add(3 * ratioBucketSpan); !s.lastWrite.Equal(want) {
		t.Fatalf("lastWrite = %v, want %v (bucket-aligned)", s.lastWrite, want)
	}
	if s.buckets[1].success != 0 || s.buckets[2].failure != 0 {
		t.Fatal("rolled-over buckets must be reset")
	}
	if s.buckets[3].drop != 1 {
		t.Fatal("the event must land in the current bucket")
	}
	if s.buckets[0].success != 1 {
		t.Fatal("the untouched oldest bucket must survive")
	}
}

// TestRatioOutcomeAccounting: client faults record healthy answers,
// gateway cuts record nothing, and a double report is absorbed.
func TestRatioOutcomeAccounting(t *testing.T) {
	t.Parallel()
	reg, clock, _ := newRatioFixture(t)
	const id = "u1"
	now := clock.Now()

	s := reg.stateOf(id, now)
	p, ok := reg.Allow(context.Background(), id)
	if !ok {
		t.Fatal("expected admission")
	}
	p.Report(OutcomeClientFault)
	p.Report(OutcomeClientFault) // absorbed

	p2, ok := reg.Allow(context.Background(), id)
	if !ok {
		t.Fatal("expected admission")
	}
	p2.Report(OutcomeGatewayTerminated)

	p3, _ := reg.Allow(context.Background(), id)
	p3.Report(OutcomeServerFault)

	h := s.history(now)
	if h.accepts != 1 || h.total != 2 {
		t.Fatalf("history = %+v, want one accept in a two-event window", h)
	}
}

// TestRatioDenialRecordedAndObserved: a denial lands in the window as
// an event and fires the observer.
func TestRatioDenialRecordedAndObserved(t *testing.T) {
	t.Parallel()
	reg, clock, draw := newRatioFixture(t)
	const id = "u1"
	now := clock.Now()

	var denied []string
	reg.onDenial = func(upstream string) { denied = append(denied, upstream) }

	feed(t, reg, id, 10)
	draw.v = 0
	if p, ok := reg.Allow(context.Background(), id); ok {
		_ = p
		t.Fatal("expected a denial")
	}
	if len(denied) != 1 || denied[0] != id {
		t.Fatalf("denial observer = %v, want exactly [%s]", denied, id)
	}

	s := reg.stateOf(id, now)
	h := s.history(now)
	if h.total != 11 {
		t.Fatalf("window total = %d, want 11 (the denial recorded)", h.total)
	}
}

// TestRatioStateOfAlwaysClosed: the ratio guard has no positions, so
// the router pre-filter and the metrics gauge read closed throughout.
func TestRatioStateOfAlwaysClosed(t *testing.T) {
	t.Parallel()
	reg, _, _ := newRatioFixture(t)

	feed(t, reg, "u1", 20)
	if state := reg.StateOf(context.Background(), "u1"); state != StateClosed {
		t.Fatalf("state = %s, want closed", state)
	}
}

// TestRatioResetEmptiesTheWindow: an operator reset admits everything
// and rebuilds the evidence.
func TestRatioResetEmptiesTheWindow(t *testing.T) {
	t.Parallel()
	reg, clock, draw := newRatioFixture(t)
	const id = "u1"

	feed(t, reg, id, 20)
	draw.v = 0
	reg.Reset(context.Background(), id)
	if p, ok := reg.Allow(context.Background(), id); !ok || p == nil {
		t.Fatal("expected admission after Reset")
	}

	s := reg.stateOf(id, clock.Now())
	h := s.history(clock.Now())
	if h.total != 0 {
		t.Fatalf("window total = %d, want 0 after Reset", h.total)
	}
}

// TestRatioConcurrentUse exercises Allow/Report/StateOf/Reset under
// contention; correctness is the race detector's job here.
func TestRatioConcurrentUse(t *testing.T) {
	t.Parallel()
	reg, _, draw := newRatioFixture(t)
	draw.v = 1

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			id := "u" + string(rune('a'+i%4))
			for j := 0; j < 50; j++ {
				if p, ok := reg.Allow(context.Background(), id); ok {
					if j%3 == 0 {
						p.Report(OutcomeServerFault)
					} else {
						p.Report(OutcomeSuccess)
					}
				}
				reg.StateOf(context.Background(), id)
				if j%25 == 0 {
					reg.Reset(context.Background(), id)
				}
			}
		}(i)
	}
	wg.Wait()
}

// TestRatioSlowOutcomeRecordsHealthy: a slow completion lands in the
// window as a healthy answer — it feeds accepts, not failures.
func TestRatioSlowOutcomeRecordsHealthy(t *testing.T) {
	t.Parallel()
	reg, clock, _ := newRatioFixture(t)
	const id = "u1"
	now := clock.Now()

	s := reg.stateOf(id, now)
	p, ok := reg.Allow(context.Background(), id)
	if !ok {
		t.Fatal("expected admission")
	}
	p.Report(OutcomeSlow)

	h := s.history(now)
	if h.accepts != 1 || h.total != 1 {
		t.Fatalf("history = %+v, want the slow completion counted healthy", h)
	}
}
