/**
 * @file slow
 * @description The slow-call-strategy breaker: a three-state machine
 * whose evidence is the share of slow completions in a rolling window,
 * for upstreams whose failure mode is degradation (every call
 * eventually answers, but responsiveness collapses long before errors
 * appear).
 *
 * Responsibilities:
 * - Open when the window's slow share reaches the configured ratio and
 *   the window holds enough samples to trust, and probe back to health
 *   exactly like the consecutive machine
 * - Nothing else: what counts as slow is the caller's classification —
 *   slow completions arrive as OutcomeSlow, carried by the report —
 *   and the breaker never reads a clock to judge speed
 *
 * Contract points:
 * - a server fault is the strongest slow evidence there is (an
 *   exchange that never completed answers no one), so it feeds both
 *   counters; a healthy fast call feeds only the total; a gateway cut
 *   feeds nothing
 * - a window with fewer samples than the minimum never opens: light
 *   traffic cannot trip the guard on a handful of slow calls
 * - closed -> open on the ratio; the cooldown and the single half-open
 *   probe follow the consecutive machine's contract, and a healthy
 *   probe closes the breaker with a freshly emptied window
 * - state is process-local; the port stays replaceable
 */
package circuit

import (
	"context"
	"sync"
	"time"
)

// Window geometry and trigger constants. The window covers ten seconds
// in forty buckets, matching the ratio strategy's granularity.
const (
	slowBuckets    = 40
	slowBucketSpan = 250 * time.Millisecond
	// slowMinSamples is the smallest window that may open the breaker:
	// below it, a few slow calls in light traffic are noise, not a
	// degradation signal.
	slowMinSamples = 10
	// defaultSlowRatio opens the breaker when half the window is slow.
	defaultSlowRatio = 0.5
)

// slowBucket is one slice of the window: completions recorded, and how
// many of them proved slow (a server fault is both — it completed for
// no one).
type slowBucket struct {
	total int64
	slow  int64
}

// slowState is the mutable per-upstream state: the machine position and
// the rolling window.
type slowState struct {
	id        string
	name      State
	openedAt  time.Time
	probe     *probeGrant
	buckets   [slowBuckets]slowBucket
	offset    int
	lastWrite time.Time
}

// SlowRegistry is the slow-call-strategy breaker registry. It is safe
// for concurrent use.
type SlowRegistry struct {
	mu         sync.Mutex
	cfg        Config
	byUpstream map[string]*slowState
	now        func() time.Time
	// onTransition observes every state change (metrics hook); nil
	// disables observation.
	onTransition func(upstreamID string, from, to State)
}

// SlowOption customizes a SlowRegistry.
type SlowOption func(*SlowRegistry)

// SlowClock overrides the clock for tests.
func SlowClock(now func() time.Time) SlowOption {
	return func(s *SlowRegistry) { s.now = now }
}

// SlowOnTransition installs the state-change observer.
func SlowOnTransition(fn func(upstreamID string, from, to State)) SlowOption {
	return func(s *SlowRegistry) { s.onTransition = fn }
}

// NewSlowRegistry builds the slow-call-strategy breaker registry. The
// config's FailThreshold is meaningless here; the trigger is the share
// of slow completions in the window, taken from SlowRatio (a value
// outside (0, 1] selects the default).
func NewSlowRegistry(cfg Config, opts ...SlowOption) *SlowRegistry {
	if cfg.SlowRatio <= 0 || cfg.SlowRatio > 1 {
		cfg.SlowRatio = defaultSlowRatio
	}
	if cfg.Cooldown <= 0 {
		cfg.Cooldown = defaultCooldown
	}
	if cfg.ProbeTimeout <= 0 {
		cfg.ProbeTimeout = defaultProbeTimeout
	}
	s := &SlowRegistry{
		cfg:        cfg,
		byUpstream: make(map[string]*slowState),
		now:        time.Now,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Allow implements Breaker: grant one call slot, or deny. The machine
// and its probe discipline are the consecutive strategy's: open denies
// unconditionally, half-open admits exactly one outstanding probe.
func (s *SlowRegistry) Allow(_ context.Context, upstreamID string) (Permission, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.stateOf(upstreamID)
	now := s.now()

	switch st.name {
	case StateClosed:
		return &slowPermission{s: s, st: st}, true

	case StateOpen:
		if now.Sub(st.openedAt) < s.cfg.Cooldown {
			return nil, false
		}
		s.transition(st, StateHalfOpen)
		grant := &probeGrant{deadline: now.Add(s.cfg.ProbeTimeout)}
		st.probe = grant
		return &slowPermission{s: s, st: st, grant: grant}, true

	case StateHalfOpen:
		if st.probe != nil {
			if now.After(st.probe.deadline) {
				s.reclaimProbe(st, now)
			}
			return nil, false
		}
		grant := &probeGrant{deadline: now.Add(s.cfg.ProbeTimeout)}
		st.probe = grant
		return &slowPermission{s: s, st: st, grant: grant}, true
	}
	return nil, false
}

// StateOf implements Breaker: reading performs the lazy transitions
// (open cooldown elapsed -> half-open; expired probe -> open) so the
// router's pre-filter never sees stale positions. A read never
// allocates the probe slot.
func (s *SlowRegistry) StateOf(_ context.Context, upstreamID string) State {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.stateOf(upstreamID)
	now := s.now()

	switch st.name {
	case StateOpen:
		if now.Sub(st.openedAt) >= s.cfg.Cooldown {
			s.transition(st, StateHalfOpen)
			return StateHalfOpen
		}
	case StateHalfOpen:
		if st.probe != nil && now.After(st.probe.deadline) {
			s.reclaimProbe(st, now)
		}
	}
	return st.name
}

// Reset implements Breaker: an operator forcing the breaker closed. The
// window empties with the machine; a granted permission still
// outstanding reports into the now-closed machine, where its outcome is
// absorbed by the ordinary closed-state rules.
func (s *SlowRegistry) Reset(_ context.Context, upstreamID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.stateOf(upstreamID)
	*st = slowState{id: st.id, name: StateClosed, lastWrite: s.now()}
}

// stateOf returns the per-upstream state, creating it in closed.
func (s *SlowRegistry) stateOf(upstreamID string) *slowState {
	st, ok := s.byUpstream[upstreamID]
	if !ok {
		st = &slowState{id: upstreamID, name: StateClosed, lastWrite: s.now()}
		s.byUpstream[upstreamID] = st
	}
	return st
}

// transition moves the machine and fires the observer.
func (s *SlowRegistry) transition(st *slowState, to State) {
	from := st.name
	if from == to {
		return
	}
	st.name = to
	if to == StateOpen {
		st.openedAt = s.now()
	}
	if to == StateClosed {
		st.probe = nil
		st.buckets = [slowBuckets]slowBucket{}
	}
	if s.onTransition != nil {
		s.onTransition(st.id, from, to)
	}
}

// reclaimProbe absorbs an abandoned or expired probe, moving half-open
// back to open with a fresh cooldown.
func (s *SlowRegistry) reclaimProbe(st *slowState, now time.Time) {
	st.probe = nil
	s.transition(st, StateOpen)
	st.openedAt = now
}

// record adds one completion to the current bucket.
func (st *slowState) record(now time.Time, slow bool) {
	elapsed := now.Sub(st.lastWrite)
	if elapsed > 0 {
		span := int(elapsed / slowBucketSpan)
		if span > slowBuckets {
			span = slowBuckets
		}
		for i := 1; i <= span; i++ {
			st.buckets[(st.offset+i)%slowBuckets] = slowBucket{}
		}
		st.offset = (st.offset + span) % slowBuckets
		st.lastWrite = st.lastWrite.Add(time.Duration(span) * slowBucketSpan)
	}
	b := &st.buckets[st.offset]
	b.total++
	if slow {
		b.slow++
	}
}

// share aggregates the live window: the completions it holds and their
// slow share.
func (st *slowState) share(now time.Time) (total int64, slow int64) {
	span := int(now.Sub(st.lastWrite) / slowBucketSpan)
	if span < 0 {
		span = 0
	}
	diff := slowBuckets - span
	if diff <= 0 {
		return 0, 0
	}
	start := (st.offset + span + 1) % slowBuckets
	for i := 0; i < diff; i++ {
		b := st.buckets[(start+i)%slowBuckets]
		total += b.total
		slow += b.slow
	}
	return total, slow
}

// slowPermission is one granted call slot. The holder reports the
// outcome exactly once; double reports are absorbed.
type slowPermission struct {
	s        *SlowRegistry
	st       *slowState
	grant    *probeGrant
	reported bool
}

// Report implements Permission: a server fault is the strongest slow
// evidence there is, a slow completion feeds both counters, a healthy
// fast call feeds only the total, and a gateway cut feeds nothing. In
// half-open, a healthy probe closes the machine on an emptied window; a
// fault re-opens it.
func (p *slowPermission) Report(outcome Outcome) {
	p.s.mu.Lock()
	defer p.s.mu.Unlock()
	if p.reported {
		return
	}
	p.reported = true
	s, st := p.s, p.st
	now := s.now()

	switch st.name {
	case StateClosed:
		switch outcome {
		case OutcomeGatewayTerminated:
			// Policy, not health evidence.
		default:
			slow := outcome == OutcomeSlow || outcome == OutcomeServerFault
			st.record(now, slow)
			total, slowCount := st.share(now)
			if total >= slowMinSamples {
				if share := float64(slowCount) / float64(total); share >= s.cfg.SlowRatio {
					s.transition(st, StateOpen)
				}
			}
		}

	case StateHalfOpen:
		if st.probe == nil || st.probe != p.grant {
			// Absorbed: the probe was already reclaimed by a concurrent
			// Allow or StateOf, or this report belongs to an earlier
			// probe and must not hijack the outstanding one.
			return
		}
		if outcome == OutcomeGatewayTerminated {
			s.reclaimProbe(st, now)
			return
		}
		if now.After(st.probe.deadline) && outcome != OutcomeServerFault {
			// A healthy probe reported past its own deadline is
			// unreliable; treat it as timed out.
			s.reclaimProbe(st, now)
			return
		}
		st.probe = nil
		if outcome == OutcomeServerFault {
			s.transition(st, StateOpen)
			return
		}
		// A healthy probe — fast or slow — closes the machine; the
		// transition empties the window, so recovery starts from clean
		// evidence instead of the backlog that opened the breaker.
		s.transition(st, StateClosed)

	case StateOpen:
		// A very late report after a concurrent reclaim: absorbed.
	}
}
