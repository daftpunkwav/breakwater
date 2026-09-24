/**
 * @file breaker
 * @description The hand-written circuit breaker: a per-upstream
 * three-state machine (closed / open / half-open).
 *
 * Responsibilities:
 * - Stop traffic toward a persistently failing upstream and probe it
 *   back to health with exactly one request
 * - Nothing else: what counts as a failure is the caller's policy
 *   (relayed through Outcome); candidate pre-filtering belongs to the
 *   router via StateOf
 *
 * Contract points (invariant I4):
 * - open denies every call without touching the upstream
 * - half-open admits exactly one outstanding probe; concurrent
 *   arrivals are denied, never queued
 * - a granted call reports its outcome exactly once; abandoned probes
 *   (panic, cancellation, hang) are reclaimed structurally — the
 *   earliest later Allow or Report observes the expired deadline and
 *   treats it as a failure, so the probe slot cannot leak
 * - state is process-local by design (PRD Q5); the port stays
 *   replaceable for a shared backend
 */
package circuit

import (
	"context"
	"sync"
	"time"
)

// Config shapes the state machine. All values must be positive; zero
// selects the defaults.
type Config struct {
	// FailThreshold is the consecutive server-fault count that opens
	// the breaker.
	FailThreshold int
	// Cooldown is how long an open breaker waits before admitting one
	// probe.
	Cooldown time.Duration
	// ProbeTimeout bounds a half-open probe; an unanswered probe counts
	// as a failure at the deadline.
	ProbeTimeout time.Duration
}

// defaults for zero Config members.
const (
	defaultFailThreshold = 5
	defaultCooldown      = 30 * time.Second
	defaultProbeTimeout  = 5 * time.Second
)

// state is the mutable per-upstream machine state.
type state struct {
	id       string
	name     State
	failures int
	openedAt time.Time
	probe    *probeGrant // non-nil while a half-open probe is outstanding
}

// probeGrant tracks the one outstanding half-open probe; nil probe
// means no probe is outstanding.
type probeGrant struct {
	deadline time.Time
}

// Registry is the process-local breaker registry. It is safe for
// concurrent use.
type Registry struct {
	mu         sync.Mutex
	cfg        Config
	byUpstream map[string]*state
	now        func() time.Time
	// onTransition observes every state change (metrics hook); nil
	// disables observation.
	onTransition func(upstreamID string, from, to State)
}

// Option customizes a Registry.
type Option func(*Registry)

// WithClock overrides the clock for tests.
func WithClock(now func() time.Time) Option {
	return func(b *Registry) { b.now = now }
}

// OnTransition installs the state-change observer.
func OnTransition(fn func(upstreamID string, from, to State)) Option {
	return func(b *Registry) { b.onTransition = fn }
}

// NewBreaker builds the breaker registry.
func NewRegistry(cfg Config, opts ...Option) *Registry {
	if cfg.FailThreshold <= 0 {
		cfg.FailThreshold = defaultFailThreshold
	}
	if cfg.Cooldown <= 0 {
		cfg.Cooldown = defaultCooldown
	}
	if cfg.ProbeTimeout <= 0 {
		cfg.ProbeTimeout = defaultProbeTimeout
	}
	b := &Registry{
		cfg:        cfg,
		byUpstream: make(map[string]*state),
		now:        time.Now,
	}
	for _, opt := range opts {
		opt(b)
	}
	return b
}

// Allow implements Breaker: grant one call slot, or deny. Denials in
// open state are unconditional; in half-open they mean another probe
// is already outstanding.
func (b *Registry) Allow(_ context.Context, upstreamID string) (Permission, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.stateOf(upstreamID)
	now := b.now()

	switch s.name {
	case StateClosed:
		return &granted{s: s, breaker: b}, true

	case StateOpen:
		if now.Sub(s.openedAt) < b.cfg.Cooldown {
			return nil, false
		}
		// Cooldown elapsed: the breaker transitions to half-open and
		// this call becomes the probe.
		b.transition(s, StateHalfOpen)
		s.probe = &probeGrant{deadline: now.Add(b.cfg.ProbeTimeout)}
		return &granted{s: s, breaker: b}, true

	case StateHalfOpen:
		// Exactly one probe may be outstanding. An expired one is
		// reclaimed as a failure first (structural guarantee): the
		// upstream hung, which is a server fault.
		if s.probe != nil {
			if now.After(s.probe.deadline) {
				b.reclaimProbe(s, now)
			}
			// Another probe is outstanding: denied, never queued.
			return nil, false
		}
		s.probe = &probeGrant{deadline: now.Add(b.cfg.ProbeTimeout)}
		return &granted{s: s, breaker: b}, true
	}
	return nil, false
}

// StateOf implements Breaker. Reading state performs lazy transitions
// (open cooldown elapsed → half-open; expired probe → open) so the
// router's pre-filter never sees stale positions.
func (b *Registry) StateOf(_ context.Context, upstreamID string) State {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.stateOf(upstreamID)
	now := b.now()

	switch s.name {
	case StateOpen:
		if now.Sub(s.openedAt) >= b.cfg.Cooldown {
			b.transition(s, StateHalfOpen)
			s.probe = &probeGrant{deadline: now.Add(b.cfg.ProbeTimeout)}
			return StateHalfOpen
		}
	case StateHalfOpen:
		if s.probe != nil && now.After(s.probe.deadline) {
			b.reclaimProbe(s, now)
		}
	}
	return s.name
}

// stateOf returns the per-upstream state, creating it in closed.
func (b *Registry) stateOf(upstreamID string) *state {
	s, ok := b.byUpstream[upstreamID]
	if !ok {
		s = &state{id: upstreamID, name: StateClosed}
		b.byUpstream[upstreamID] = s
	}
	return s
}

// transition moves the machine and fires the observer.
func (b *Registry) transition(s *state, to State) {
	from := s.name
	if from == to {
		return
	}
	s.name = to
	if to == StateOpen {
		s.openedAt = b.now()
	}
	if to == StateClosed {
		s.failures = 0
		s.probe = nil
	}
	if b.onTransition != nil {
		b.onTransition(s.id, from, to)
	}
}

// reclaimProbe absorbs an abandoned or expired probe as a server
// fault, moving half-open back to open with a fresh cooldown.
func (b *Registry) reclaimProbe(s *state, now time.Time) {
	s.probe = nil
	b.transition(s, StateOpen)
	s.openedAt = now
}

// granted is the Permission of an admitted call.
type granted struct {
	breaker *Registry
	s       *state
	// reported guards the exactly-once rule against double reports.
	reported bool
}

// Report implements Permission: feeds the outcome into the machine.
// The caller must hold no locks; the breaker takes its own.
func (g *granted) Report(outcome Outcome) {
	g.breaker.mu.Lock()
	defer g.breaker.mu.Unlock()
	if g.reported {
		return
	}
	g.reported = true

	b, s := g.breaker, g.s
	now := b.now()

	switch s.name {
	case StateClosed:
		switch outcome {
		case OutcomeSuccess, OutcomeClientFault:
			s.failures = 0
		case OutcomeServerFault:
			s.failures++
			if s.failures >= b.cfg.FailThreshold {
				b.transition(s, StateOpen)
			}
		}

	case StateHalfOpen:
		if s.probe == nil {
			// Absorbed: the probe was already reclaimed by a concurrent
			// Allow or StateOf.
			return
		}
		if now.After(s.probe.deadline) && outcome == OutcomeSuccess {
			// A success reported after its own deadline is unreliable;
			// treat the probe as timed out.
			b.reclaimProbe(s, now)
			return
		}
		s.probe = nil
		switch outcome {
		case OutcomeSuccess, OutcomeClientFault:
			b.transition(s, StateClosed)
		case OutcomeServerFault:
			b.transition(s, StateOpen)
		}

	case StateOpen:
		// A very late report after a concurrent reclaim: absorbed.
	}
}
