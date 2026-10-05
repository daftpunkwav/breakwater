/**
 * @file breaker
 * @description The hand-written consecutive-failure circuit breaker: a
 * per-upstream three-state machine (closed / open / half-open) over the
 * positional skeleton every strategy shares (machine.go).
 *
 * Responsibilities:
 * - Stop traffic toward a persistently failing upstream and probe it
 *   back to health with exactly one request
 * - Nothing else: what counts as a failure is the caller's policy
 *   (relayed through Outcome); candidate pre-filtering belongs to the
 *   router via StateOf
 *
 * Contract points:
 * - open denies every call without touching the upstream
 * - half-open admits exactly one outstanding probe; concurrent
 *   arrivals are denied, never queued
 * - a granted call reports its outcome exactly once; a probe whose
 *   holder never reports is reclaimed when a later Allow, StateOf or
 *   Report observes the expired deadline, so the probe slot cannot leak
 *   for longer than the probe timeout. Nothing detects a panic or a
 *   cancellation directly — an expired deadline is the only signal.
 * - state is process-local; the port stays
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
	// SlowRatio drives the slow-call strategy only: the share of slow
	// completions in the rolling window that opens the breaker. A value
	// outside (0, 1] selects the default.
	SlowRatio float64
}

// Config values substituted for zero members.
const (
	defaultFailThreshold = 5
	defaultCooldown      = 30 * time.Second
	defaultProbeTimeout  = 5 * time.Second
)

// state is the mutable per-upstream machine state: the shared skeleton
// plus the consecutive-failure count that is this strategy's evidence.
type state struct {
	machine
	failures int
}

// probeGrant tracks the one outstanding half-open probe; nil probe
// means no probe is outstanding.
type probeGrant struct {
	deadline time.Time
}

// Registry is the process-local breaker registry. It is safe for
// concurrent use.
type Registry struct {
	mu sync.Mutex
	// gov runs the positional skeleton; failThreshold is this
	// strategy's evidence rule.
	gov           governor
	failThreshold int
	byUpstream    map[string]*state
}

// Option customizes a Registry.
type Option func(*Registry)

// WithClock overrides the clock for tests.
func WithClock(now func() time.Time) Option {
	return func(b *Registry) { b.gov.now = now }
}

// OnTransition installs the state-change observer.
func OnTransition(fn func(upstreamID string, from, to State)) Option {
	return func(b *Registry) { b.gov.onTransition = fn }
}

// NewRegistry builds the breaker registry.
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
		gov: governor{
			cooldown:     cfg.Cooldown,
			probeTimeout: cfg.ProbeTimeout,
			now:          time.Now,
		},
		failThreshold: cfg.FailThreshold,
		byUpstream:    make(map[string]*state),
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
	return b.gov.allow(&s.machine,
		func(grant *probeGrant) Permission { return &granted{s: s, breaker: b, grant: grant} })
}

// StateOf implements Breaker. Reading state performs lazy transitions
// (open cooldown elapsed → half-open; expired probe → open) so the
// router's pre-filter never sees stale positions.
func (b *Registry) StateOf(_ context.Context, upstreamID string) State {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.stateOf(upstreamID)
	return b.gov.observe(&s.machine)
}

// Reset implements Breaker: an operator forcing the breaker closed.
// The failure count and any outstanding probe are cleared; a granted
// permission still outstanding reports into the now-closed machine,
// where its outcome is absorbed by the ordinary closed-state rules.
func (b *Registry) Reset(_ context.Context, upstreamID string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	s := b.stateOf(upstreamID)
	s.failures = 0
	s.probe = nil
	b.gov.enter(&s.machine, StateClosed, nil)
}

// stateOf returns the per-upstream state, creating it in closed.
func (b *Registry) stateOf(upstreamID string) *state {
	s, ok := b.byUpstream[upstreamID]
	if !ok {
		s = &state{machine: machine{id: upstreamID, name: StateClosed}}
		b.byUpstream[upstreamID] = s
	}
	return s
}

// granted is the Permission of an admitted call. grant pins the
// half-open probe slot this permission owns (nil outside half-open), so
// a very late report cannot act on a probe granted to a later caller.
type granted struct {
	breaker *Registry
	s       *state
	grant   *probeGrant
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
	now := b.gov.now()

	switch s.name {
	case StateClosed:
		switch outcome {
		case OutcomeSuccess, OutcomeClientFault, OutcomeSlow:
			s.failures = 0
		case OutcomeServerFault:
			s.failures++
			if s.failures >= b.failThreshold {
				b.gov.enter(&s.machine, StateOpen, nil)
			}
		default:
			// OutcomeGatewayTerminated: the gateway cut the call under
			// its own policy — neither success nor failure evidence.
			// The counter stays exactly as it is.
		}

	case StateHalfOpen:
		if s.probe == nil || s.probe != g.grant {
			// Absorbed: the probe was already reclaimed by a concurrent
			// Allow or StateOf, or this report belongs to an earlier
			// probe and must not hijack the outstanding one.
			return
		}
		if outcome == OutcomeGatewayTerminated {
			// A probe the gateway itself truncated proves nothing about
			// the upstream: back to open with a fresh cooldown, exactly
			// like an expired probe, instead of closing on no evidence.
			b.gov.reclaim(&s.machine, now)
			return
		}
		if now.After(s.probe.deadline) && (outcome == OutcomeSuccess || outcome == OutcomeSlow) {
			// A success reported after its own deadline is unreliable;
			// treat the probe as timed out.
			b.gov.reclaim(&s.machine, now)
			return
		}
		s.probe = nil
		switch outcome {
		case OutcomeSuccess, OutcomeClientFault, OutcomeSlow:
			b.gov.enter(&s.machine, StateClosed, func() { s.failures = 0 })
		case OutcomeServerFault:
			b.gov.enter(&s.machine, StateOpen, nil)
		}

	case StateOpen:
		// A very late report after a concurrent reclaim: absorbed.
	}
}
