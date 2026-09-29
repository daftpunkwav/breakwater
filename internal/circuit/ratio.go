/**
 * @file ratio
 * @description The ratio-strategy breaker: an accept-ratio probability
 * guard over a rolling window, an alternative to the consecutive-failure
 * state machine for upstreams whose failures are proportional (a
 * partially saturated backend degrades some requests while others
 * succeed).
 *
 * Responsibilities:
 * - Deny a rising fraction of calls as the window's failure share
 *   grows, and admit everything again as healthy answers accumulate
 * - Never fully cut traffic: one call per forced-pass interval gets
 *   through no matter how saturated the window is, so recovery is
 *   observable without operator action
 * - Nothing else: what counts as a failure is the caller's policy
 *   (relayed through Outcome), exactly like the consecutive machine
 *
 * Contract points:
 * - the guard is a probability, not a position: StateOf always reads
 *   closed, no transition ever fires, and the router's pre-filter
 *   keeps every candidate eligible — a denial happens per call in
 *   Allow, and a denied call is itself recorded in the window, which
 *   is what keeps the deny probability up while the failure persists
 * - a window with no recent events empties out and admits everything,
 *   so idle time never keeps a grudge
 * - client faults count as healthy answers (the upstream is clearly
 *   alive to reject authorization) and a gateway-terminated call is
 *   recorded by nothing — it is policy, not health evidence
 * - state is process-local; the port stays replaceable
 */
package circuit

import (
	"context"
	"math"
	"math/rand/v2"
	"sync"
	"time"
)

// Window geometry and formula constants. The window covers ten seconds
// in forty buckets; the constants shape how aggressively the deny
// probability rises and how much healthy history forgives.
const (
	ratioBuckets    = 40
	ratioBucketSpan = 250 * time.Millisecond
	// ratioK is the accept multiplier a fully healthy window grants:
	// accepts are trusted to justify more traffic than they observed.
	ratioK = 1.5
	// ratioMinK floors the multiplier: a trailing all-failure streak
	// discounts past accepts, but never below this.
	ratioMinK = 1.1
	// ratioProtection keeps tiny windows honest: five or fewer recorded
	// events can never deny anything, so one unlucky failure in light
	// traffic does not trip the guard.
	ratioProtection = 5
	// ratioForcePass is the forced-pass interval: while the guard would
	// deny, one call per interval is admitted anyway. Its outcome is
	// the fresh evidence that lets the ratio relax again.
	ratioForcePass = time.Second
)

// ratioBucket is one slice of the window: healthy answers, upstream
// failures, and denials this guard itself produced.
type ratioBucket struct {
	success int64
	failure int64
	drop    int64
}

// ratioState is the per-upstream window plus the forced-pass cursor.
type ratioState struct {
	id        string
	buckets   [ratioBuckets]ratioBucket
	offset    int       // the current (newest) bucket
	lastWrite time.Time // start of the bucket at offset
	lastPass  time.Time // last admission made while the guard denied
}

// ratioHistory aggregates one read of the window.
type ratioHistory struct {
	total          int64 // every recorded event, denials included
	accepts        int64 // healthy answers
	failingBuckets int   // trailing run of failure-only buckets
	workingBuckets int   // trailing run of failure-free buckets
}

// RatioRegistry is the ratio-strategy breaker registry. It is safe for
// concurrent use.
type RatioRegistry struct {
	mu         sync.Mutex
	byUpstream map[string]*ratioState
	now        func() time.Time
	random     func() float64
	// onDenial observes every probabilistic denial (metrics hook); nil
	// disables observation.
	onDenial func(upstreamID string)
}

// RatioOption customizes a RatioRegistry.
type RatioOption func(*RatioRegistry)

// RatioClock overrides the clock for tests.
func RatioClock(now func() time.Time) RatioOption {
	return func(r *RatioRegistry) { r.now = now }
}

// RatioRandomSource overrides the uniform sampler for tests; a draw
// below the computed deny ratio denies the call.
func RatioRandomSource(random func() float64) RatioOption {
	return func(r *RatioRegistry) { r.random = random }
}

// RatioOnDenial installs the denial observer.
func RatioOnDenial(fn func(upstreamID string)) RatioOption {
	return func(r *RatioRegistry) { r.onDenial = fn }
}

// NewRatioRegistry builds the ratio-strategy breaker registry.
func NewRatioRegistry(opts ...RatioOption) *RatioRegistry {
	r := &RatioRegistry{
		byUpstream: make(map[string]*ratioState),
		now:        time.Now,
		random:     rand.Float64,
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Allow implements Breaker: admit the call, or deny it with the
// window's deny probability. The probability grows with the failure
// share, is diluted by trailing healthy buckets, and is zero for a
// window too small to trust.
func (r *RatioRegistry) Allow(_ context.Context, upstreamID string) (Permission, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	s := r.stateOf(upstreamID, now)
	h := s.history(now)

	// A trailing all-failure streak discounts past accepts: the longer
	// the healthy answers stay absent, the less each one still counts.
	weight := ratioK - (ratioK-ratioMinK)*(float64(h.failingBuckets)/ratioBuckets)
	weightedAccepts := math.Max(weight, ratioMinK) * float64(h.accepts)
	dropRatio := (float64(h.total-ratioProtection) - weightedAccepts) / float64(h.total+1)
	if dropRatio <= 0 {
		return &ratioPermission{s: s, reg: r}, true
	}
	// Recovery guarantee: past the forced-pass interval one call is
	// admitted whatever the ratio says. Without it, a fully saturated
	// window could keep itself saturated forever — denials would
	// sustain the very evidence that causes them.
	if !s.lastPass.IsZero() && now.Sub(s.lastPass) >= ratioForcePass {
		s.lastPass = now
		return &ratioPermission{s: s, reg: r}, true
	}
	// Healthy buckets dilute the deny ratio: a single bad bucket inside
	// an otherwise clean window must not deny most traffic.
	dropRatio *= float64(ratioBuckets-h.workingBuckets) / ratioBuckets
	if r.random() < dropRatio {
		// The denial is itself an event: a sustained failure keeps the
		// window saturated even when nothing new reaches the upstream.
		s.add(now, 0, 0, 1)
		if r.onDenial != nil {
			r.onDenial(s.id)
		}
		return nil, false
	}
	s.lastPass = now
	return &ratioPermission{s: s, reg: r}, true
}

// StateOf implements Breaker: the ratio guard has no positions, so it
// always reads closed — the deny probability lives in Allow alone.
func (r *RatioRegistry) StateOf(_ context.Context, _ string) State {
	return StateClosed
}

// Reset implements Breaker: an operator clearing the guard. The window
// empties, so the next call is admitted unconditionally and the
// evidence rebuilds from scratch.
func (r *RatioRegistry) Reset(_ context.Context, upstreamID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := r.now()
	s := r.stateOf(upstreamID, now)
	s.buckets = [ratioBuckets]ratioBucket{}
	s.offset = 0
	s.lastWrite = now
	s.lastPass = time.Time{}
}

// stateOf returns the per-upstream window, creating it aligned to now.
func (r *RatioRegistry) stateOf(upstreamID string, now time.Time) *ratioState {
	s, ok := r.byUpstream[upstreamID]
	if !ok {
		s = &ratioState{id: upstreamID, lastWrite: now}
		r.byUpstream[upstreamID] = s
	}
	return s
}

// history aggregates the window oldest-first without touching it:
// expired buckets are skipped by span, never reset — only a write
// rolls the ring. The failing/working streaks walk the same order, so
// their final values describe the run ending at the current bucket.
func (s *ratioState) history(now time.Time) ratioHistory {
	var h ratioHistory
	span := int(now.Sub(s.lastWrite) / ratioBucketSpan)
	if span < 0 {
		span = 0
	}
	diff := ratioBuckets - span
	if diff <= 0 {
		return h
	}
	start := (s.offset + span + 1) % ratioBuckets
	for i := 0; i < diff; i++ {
		b := s.buckets[(start+i)%ratioBuckets]
		h.total += b.success + b.failure + b.drop
		h.accepts += b.success
		if b.failure > 0 {
			h.workingBuckets = 0
		} else if b.success > 0 {
			h.workingBuckets++
		}
		if b.success > 0 {
			h.failingBuckets = 0
		} else if b.failure > 0 {
			h.failingBuckets++
		}
	}
	return h
}

// add rolls the ring forward and records events in the current bucket.
func (s *ratioState) add(now time.Time, success, failure, drop int64) {
	s.roll(now)
	b := &s.buckets[s.offset]
	b.success += success
	b.failure += failure
	b.drop += drop
}

// roll advances the ring to now's bucket, resetting every bucket the
// cursor passes. lastWrite stays aligned to a bucket boundary, so the
// remainder of the elapsed span stays inside the current bucket and
// boundaries never drift.
func (s *ratioState) roll(now time.Time) {
	elapsed := now.Sub(s.lastWrite)
	if elapsed <= 0 {
		return
	}
	span := int(elapsed / ratioBucketSpan)
	if span > ratioBuckets {
		span = ratioBuckets
	}
	for i := 1; i <= span; i++ {
		s.buckets[(s.offset+i)%ratioBuckets] = ratioBucket{}
	}
	s.offset = (s.offset + span) % ratioBuckets
	s.lastWrite = s.lastWrite.Add(time.Duration(span) * ratioBucketSpan)
}

// ratioPermission is one admitted call. The holder reports the outcome
// of the call exactly once; double reports are absorbed.
type ratioPermission struct {
	reg      *RatioRegistry
	s        *ratioState
	reported bool
}

// Report implements Permission: successes and client faults record
// healthy answers, server faults record failures, and a gateway cut
// records nothing. A late report lands in whatever bucket is current
// when it arrives — the ring can only ever record the present.
func (p *ratioPermission) Report(outcome Outcome) {
	p.reg.mu.Lock()
	defer p.reg.mu.Unlock()
	if p.reported {
		return
	}
	p.reported = true
	now := p.reg.now()
	switch outcome {
	case OutcomeSuccess, OutcomeClientFault:
		p.s.add(now, 1, 0, 0)
	case OutcomeServerFault:
		p.s.add(now, 0, 1, 0)
	default:
		// OutcomeGatewayTerminated: policy, not health evidence.
	}
}
