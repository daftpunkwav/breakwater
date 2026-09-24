/**
 * @file breaker_test
 * @description Breaker state machine tests: every transition path of
 * the frozen design, concurrent probe exclusivity (invariant I4) and
 * the structural reclaim of abandoned probes.
 */
package circuit

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// testRegistry builds an isolated breaker with a movable clock; the
// returned func advances time for that test alone (parallel tests must
// not share a clock).
func testRegistry(t *testing.T, mutate func(*Config), opts ...Option) (*Registry, func(time.Duration)) {
	t.Helper()
	cfg := Config{FailThreshold: 3, Cooldown: time.Second, ProbeTimeout: time.Second}
	if mutate != nil {
		mutate(&cfg)
	}
	clock := time.Now()
	b := NewRegistry(cfg, append(opts, WithClock(func() time.Time { return clock }))...)
	return b, func(d time.Duration) { clock = clock.Add(d) }
}

func TestBreakerOpensAfterSustainedFailures(t *testing.T) {
	t.Parallel()
	b, _ := testRegistry(t, nil)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		perm, ok := b.Allow(ctx, "u")
		if !ok {
			t.Fatalf("call %d denied in closed state", i)
		}
		perm.Report(OutcomeServerFault)
	}
	if got := b.StateOf(ctx, "u"); got != StateOpen {
		t.Fatalf("state = %s, want open after threshold", got)
	}
	// Open: every call denied.
	if _, ok := b.Allow(ctx, "u"); ok {
		t.Fatal("open breaker granted a call (I4)")
	}
}

func TestBreakerSuccessResetsFailureCount(t *testing.T) {
	t.Parallel()
	b, _ := testRegistry(t, nil)
	ctx := context.Background()

	for range 2 {
		p, _ := b.Allow(ctx, "u")
		p.Report(OutcomeServerFault)
	}
	p, _ := b.Allow(ctx, "u")
	p.Report(OutcomeSuccess)
	// The count restarted: two more faults stay under the threshold of 3.
	for range 2 {
		p, _ = b.Allow(ctx, "u")
		p.Report(OutcomeServerFault)
	}
	if got := b.StateOf(ctx, "u"); got != StateClosed {
		t.Fatalf("state = %s, want closed: success must reset the count", got)
	}
}

func TestBreakerClientFaultNeverCounts(t *testing.T) {
	t.Parallel()
	b, _ := testRegistry(t, nil)
	ctx := context.Background()

	for range 10 {
		p, _ := b.Allow(ctx, "u")
		p.Report(OutcomeClientFault)
	}
	if got := b.StateOf(ctx, "u"); got != StateClosed {
		t.Fatalf("state = %s, want closed: client faults are not upstream faults", got)
	}
}

func TestBreakerHalfOpenAdmitsSingleProbe(t *testing.T) {
	t.Parallel()
	b, advance := testRegistry(t, nil)
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
	// Concurrent arrivals are denied, never queued.
	for range 5 {
		if _, ok := b.Allow(ctx, "u"); ok {
			t.Fatal("half-open admitted a second concurrent call (I4)")
		}
	}
	probe.Report(OutcomeSuccess)
	if got := b.StateOf(ctx, "u"); got != StateClosed {
		t.Fatalf("state = %s, want closed after successful probe", got)
	}
}

func TestBreakerFailedProbeReopens(t *testing.T) {
	t.Parallel()
	b, advance := testRegistry(t, nil)
	ctx := context.Background()

	for range 3 {
		p, _ := b.Allow(ctx, "u")
		p.Report(OutcomeServerFault)
	}
	advance(2 * time.Second)
	probe, ok := b.Allow(ctx, "u")
	if !ok {
		t.Fatal("half-open denied the probe")
	}
	probe.Report(OutcomeServerFault)
	if got := b.StateOf(ctx, "u"); got != StateOpen {
		t.Fatalf("state = %s, want open after failed probe", got)
	}
	// The cooldown restarts from the reopen.
	if _, ok := b.Allow(ctx, "u"); ok {
		t.Fatal("open breaker granted a call right after reopen")
	}
}

func TestBreakerReclaimsAbandonedProbe(t *testing.T) {
	t.Parallel()
	b, advance := testRegistry(t, nil)
	ctx := context.Background()

	for range 3 {
		p, _ := b.Allow(ctx, "u")
		p.Report(OutcomeServerFault)
	}
	advance(2 * time.Second)
	if _, ok := b.Allow(ctx, "u"); !ok {
		t.Fatal("half-open denied the probe")
	}
	// The probe holder vanishes (panic, hang): time passes the probe
	// deadline without a Report.
	advance(2 * time.Second)

	// The next Allow observes the expired probe structurally.
	if _, ok := b.Allow(ctx, "u"); ok {
		t.Fatal("grant after abandoned probe must be denied")
	}
	if got := b.StateOf(ctx, "u"); got != StateOpen {
		t.Fatalf("state = %s, want open: the probe slot must not leak (I4)", got)
	}
}

func TestBreakerDoubleReportAbsorbed(t *testing.T) {
	t.Parallel()
	b, advance := testRegistry(t, nil)
	ctx := context.Background()

	for range 3 {
		p, _ := b.Allow(ctx, "u")
		p.Report(OutcomeServerFault)
	}
	advance(2 * time.Second)
	probe, _ := b.Allow(ctx, "u")
	probe.Report(OutcomeSuccess) // closes
	probe.Report(OutcomeServerFault)
	if got := b.StateOf(ctx, "u"); got != StateClosed {
		t.Fatalf("state = %s, want closed: double reports must be absorbed", got)
	}
}

func TestBreakerTransitionsAreObserved(t *testing.T) {
	t.Parallel()
	var transitions []string
	b, advance := testRegistry(t, func(c *Config) { c.FailThreshold = 1 },
		OnTransition(func(id string, from, to State) {
			transitions = append(transitions, string(from)+"->"+string(to))
		}))
	ctx := context.Background()

	p, _ := b.Allow(ctx, "u")
	p.Report(OutcomeServerFault) // closed -> open
	advance(2 * time.Second)
	probe, _ := b.Allow(ctx, "u")
	probe.Report(OutcomeServerFault) // half-open -> open
	advance(2 * time.Second)
	probe, _ = b.Allow(ctx, "u")
	probe.Report(OutcomeSuccess) // half-open -> closed

	want := []string{"closed->open", "open->half-open", "half-open->open", "open->half-open", "half-open->closed"}
	if len(transitions) != len(want) {
		t.Fatalf("transitions = %v, want %v", transitions, want)
	}
	for i := range want {
		if transitions[i] != want[i] {
			t.Fatalf("transition %d = %s, want %s", i, transitions[i], want[i])
		}
	}
}

// TestBreakerReadThenAllowProbes reproduces the production sequence —
// the router reads StateOf, then the attempt calls Allow — and proves a
// read cannot consume the probe slot: after the cooldown the breaker
// must still admit the probe and recover to closed.
func TestBreakerReadThenAllowProbes(t *testing.T) {
	t.Parallel()
	b, advance := testRegistry(t, nil)
	ctx := context.Background()

	for range 3 {
		p, _ := b.Allow(ctx, "u")
		p.Report(OutcomeServerFault)
	}
	advance(2 * time.Second) // cooldown elapses

	// The router's pre-filter sees half-open and keeps the upstream as a
	// candidate.
	if got := b.StateOf(ctx, "u"); got != StateHalfOpen {
		t.Fatalf("state = %s, want half-open after the cooldown", got)
	}
	// The attempt right after the read must get the probe slot.
	probe, ok := b.Allow(ctx, "u")
	if !ok {
		t.Fatal("Allow denied right after a StateOf read: the read consumed the probe slot")
	}
	probe.Report(OutcomeSuccess)
	if got := b.StateOf(ctx, "u"); got != StateClosed {
		t.Fatalf("state = %s, want closed after the probe", got)
	}
}

// TestBreakerConcurrentProbesExactlyOne is the I4 concurrency evidence
// under -race: a stampede against a half-open breaker must grant
// exactly one probe.
func TestBreakerConcurrentProbesExactlyOne(t *testing.T) {
	t.Parallel()
	b, advance := testRegistry(t, nil)
	ctx := context.Background()

	for range 3 {
		p, _ := b.Allow(ctx, "u")
		p.Report(OutcomeServerFault)
	}
	advance(2 * time.Second)

	var granted atomic.Int64
	var wg sync.WaitGroup
	for range 64 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, ok := b.Allow(ctx, "u"); ok {
				granted.Add(1)
			}
		}()
	}
	wg.Wait()
	if got := granted.Load(); got != 1 {
		t.Fatalf("granted = %d probes, want exactly 1", got)
	}
}

// TestBreakerStaleProbeReportDoesNotHijack locks the probe-identity
// rule: a report from a probe whose slot was already reclaimed must be
// absorbed instead of acting on the probe granted to a later caller —
// otherwise a stale success could close the breaker while the live
// probe is still in flight (I4's exactly-one-probe guarantee).
func TestBreakerStaleProbeReportDoesNotHijack(t *testing.T) {
	t.Parallel()
	b, advance := testRegistry(t, nil)
	ctx := context.Background()

	for range 3 {
		p, _ := b.Allow(ctx, "u")
		p.Report(OutcomeServerFault)
	}
	advance(2 * time.Second)
	stale, ok := b.Allow(ctx, "u") // probe A
	if !ok {
		t.Fatal("half-open denied the first probe")
	}
	advance(2 * time.Second) // probe A's deadline expires unreclaimed

	// The next Allow reclaims A as a failure (denied here), then the
	// cooldown admits probe B.
	if _, ok := b.Allow(ctx, "u"); ok {
		t.Fatal("grant after an expired probe must be denied")
	}
	advance(2 * time.Second)
	fresh, ok := b.Allow(ctx, "u") // probe B
	if !ok {
		t.Fatal("half-open denied the replacement probe")
	}

	stale.Report(OutcomeSuccess) // A's very late success
	if got := b.StateOf(ctx, "u"); got != StateHalfOpen {
		t.Fatalf("state = %s, want half-open: a stale report must not close the breaker", got)
	}
	fresh.Report(OutcomeServerFault)
	if got := b.StateOf(ctx, "u"); got != StateOpen {
		t.Fatalf("state = %s, want open: the live probe's outcome must drive the machine", got)
	}
}

func TestBreakerUpstreamsAreIndependent(t *testing.T) {
	t.Parallel()
	b, _ := testRegistry(t, nil)
	ctx := context.Background()

	for range 3 {
		p, _ := b.Allow(ctx, "a")
		p.Report(OutcomeServerFault)
	}
	if got := b.StateOf(ctx, "a"); got != StateOpen {
		t.Fatalf("a = %s, want open", got)
	}
	if got := b.StateOf(ctx, "b"); got != StateClosed {
		t.Fatalf("b = %s, want closed", got)
	}
}
