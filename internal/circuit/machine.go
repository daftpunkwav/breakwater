/**
 * @file machine
 * @description The three-state skeleton both hand-written breaker
 * strategies run (the consecutive-failure registry and the slow-call
 * registry): the position, the cooldown gate out of open and the
 * exactly-one-probe discipline of half-open, in one place so the two
 * registries cannot drift.
 *
 * Responsibilities:
 * - Move a machine between closed, open and half-open, firing the
 *   transition observer and stamping the open moment
 * - Admit or deny calls: open denies until its cooldown elapses, then
 *   the caller becomes the probe; half-open admits only when no probe
 *   is outstanding, reclaiming an expired one first
 * - Nothing else: what counts as failure or slow evidence is each
 *   strategy's Report rule; the skeleton never judges an outcome, and
 *   the ratio strategy runs no state machine at all
 *
 * Contract points:
 * - a read (observe) performs the lazy transitions but never allocates
 *   the probe slot — probing stays the exclusive business of Allow
 * - a reclaimed probe counts as a server fault: past its deadline a
 *   probe is no longer evidence about the upstream, whoever still
 *   holds it — a hung holder and a holder that finished successfully
 *   but reported late look the same here
 * - the governor takes no locks of its own: the owning registry holds
 *   its mutex across every call
 */
package circuit

import (
	"time"
)

// machine is the mutable positional state of one upstream's breaker:
// the strategy-specific evidence (a failure count, a sample window)
// lives beside it in the owning registry's own state struct.
type machine struct {
	id       string
	name     State
	openedAt time.Time
	probe    *probeGrant // non-nil while a half-open probe is outstanding
}

// governor drives machines through the shared positional mechanics:
// the cooldown and probe timeouts, the clock and the transition
// observer. The strategies contribute their own evidence rules and
// their own closed-entry cleanup.
type governor struct {
	cooldown     time.Duration
	probeTimeout time.Duration
	now          func() time.Time
	// onTransition observes every state change (metrics hook); nil
	// disables observation.
	onTransition func(upstreamID string, from, to State)
}

// allow applies the shared admission rule to one call. admit builds the
// strategy's permission; grant is nil outside half-open. onClosed is
// the strategy's own cleanup on entering closed (see enter).
func (g *governor) allow(m *machine, admit func(*probeGrant) Permission, onClosed func()) (Permission, bool) {
	now := g.now()
	switch m.name {
	case StateClosed:
		return admit(nil), true

	case StateOpen:
		if now.Sub(m.openedAt) < g.cooldown {
			return nil, false
		}
		// Cooldown elapsed: the breaker transitions to half-open and
		// this call becomes the probe.
		g.enter(m, StateHalfOpen, onClosed)
		grant := &probeGrant{deadline: now.Add(g.probeTimeout)}
		m.probe = grant
		return admit(grant), true

	case StateHalfOpen:
		// Exactly one probe may be outstanding. An expired one is
		// reclaimed first, then the call is still denied: this arrival
		// must not inherit the slot the dead probe held.
		if m.probe != nil {
			if now.After(m.probe.deadline) {
				g.reclaim(m, now, onClosed)
			}
			// Another probe is outstanding: denied, never queued.
			return nil, false
		}
		grant := &probeGrant{deadline: now.Add(g.probeTimeout)}
		m.probe = grant
		return admit(grant), true
	}
	return nil, false
}

// observe applies the shared lazy transitions for a read — open whose
// cooldown elapsed moves to half-open, an expired probe is reclaimed —
// so a router's pre-filter never sees stale positions. A read never
// allocates the probe slot, so a read followed by the attempt still
// finds the slot free.
func (g *governor) observe(m *machine, onClosed func()) State {
	now := g.now()
	switch m.name {
	case StateOpen:
		if now.Sub(m.openedAt) >= g.cooldown {
			g.enter(m, StateHalfOpen, onClosed)
			return StateHalfOpen
		}
	case StateHalfOpen:
		if m.probe != nil && now.After(m.probe.deadline) {
			g.reclaim(m, now, onClosed)
		}
	}
	return m.name
}

// enter moves the machine and fires the observer. Entering open stamps
// openedAt; entering closed clears the probe slot and then runs
// onClosed, the strategy's own cleanup — the consecutive strategy
// drops its failure count, the slow strategy empties its window. The
// observer fires last, after the machine is fully settled.
func (g *governor) enter(m *machine, to State, onClosed func()) {
	from := m.name
	if from == to {
		return
	}
	m.name = to
	if to == StateOpen {
		m.openedAt = g.now()
	}
	if to == StateClosed {
		m.probe = nil
		if onClosed != nil {
			onClosed()
		}
	}
	if g.onTransition != nil {
		g.onTransition(m.id, from, to)
	}
}

// reclaim absorbs an abandoned or expired probe as a server fault,
// moving half-open back to open with a fresh cooldown stamped from the
// observation moment.
func (g *governor) reclaim(m *machine, now time.Time, onClosed func()) {
	m.probe = nil
	g.enter(m, StateOpen, onClosed)
	m.openedAt = now
}
