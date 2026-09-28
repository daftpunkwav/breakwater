/**
 * @file retry
 * @description Retry protocol contracts: attempt layering and budgets.
 *
 * Responsibilities:
 * - Own the attempt loop shape: a client request decomposes into
 *   attempts, each bounded by AttemptTimeout, all together by
 *   OverallDeadline, in count by MaxAttempts
 * - Nothing else: error classification tables and backoff math are
 *   implementation details supplied to the loop
 *
 * Contract points:
 * - A request never exceeds its attempt cap, and retries never
 *   exceed the global in-flight budget (retry storm containment)
 * - The attempt loop — not the classifier — owns the first-byte
 *   boundary. After the first response byte has reached the client the
 *   loop must not consult the classifier at all; such failures
 *   terminate via the SSE error event contract. Emitting that event
 *   belongs to the forward stage that owns the client response;
 *   deciding that no attempt remains belongs here.
 */
package retry

import (
	"fmt"
	"sync"
	"time"
)

// Policy bounds a single client request.
type Policy struct {
	// MaxAttempts caps upstream attempts per client request.
	MaxAttempts int
	// AttemptTimeout bounds one upstream attempt.
	AttemptTimeout time.Duration
	// OverallDeadline bounds all attempts of one client request together.
	OverallDeadline time.Duration
	// BackoffInitial and BackoffMax shape the exponential backoff; jitter
	// policy is an implementation detail.
	BackoffInitial time.Duration
	BackoffMax     time.Duration
}

// Classifier reports whether an error kind is retryable in principle:
// connection failures, 429 and 5xx exchanges and timeouts are; client
// errors (4xx) are not.
//
// The classifier sees errors only. Positional gating (nothing is
// retryable after the first response byte) is enforced by the
// attempt loop before the classifier is ever consulted.
type Classifier interface {
	Retryable(err error) bool
}

// Budget is the process-wide in-flight retry budget shared by all
// requests; it bounds how much of total traffic is retry amplification
// (retry storm containment). It is a concrete type on purpose: a
// single in-process counter with no foreseeable alternative backend,
// unlike the breaker whose backend stays replaceable.
//
// Two admission modes exist. The static mode caps retries at a fixed
// number. The share mode scales the cap with live traffic — a fixed
// share of the requests currently in flight, floored at a minimum —
// so a fixed-percentage share of traffic may be retry amplification
// whatever the load: under a lull the cap shrinks (a small pool
// cannot justify many retries), under pressure it grows. The share
// mode replaces the static cap entirely; it never combines with one.
type Budget struct {
	mu       sync.Mutex
	inFlight int
	max      int
	// Share mode; percent 0 selects the static cap.
	percent     int
	minInFlight int
	// inflightTotal reports the requests currently in flight across
	// the process. Installed only in share mode; never nil there.
	inflightTotal func() int64
}

// NewBudget builds a budget admitting at most maxInFlight concurrent
// retry attempts.
func NewBudget(maxInFlight int) *Budget {
	if maxInFlight < 0 {
		maxInFlight = 0
	}
	return &Budget{max: maxInFlight}
}

// NewShareBudget builds a budget whose cap is recomputed at every
// admission as max(minInFlight, percent of the requests currently in
// flight): retries may occupy at most that share of live traffic. The
// traffic source is mandatory: without it the share is uncomputable
// and admissions could only panic, so the constructor refuses nil
// outright instead of pretending.
func NewShareBudget(percent, minInFlight int, inflightTotal func() int64) (*Budget, error) {
	if inflightTotal == nil {
		return nil, fmt.Errorf("retry: share budget needs an in-flight source")
	}
	if percent <= 0 || percent > 100 {
		return nil, fmt.Errorf("retry: share budget percent %d out of range [1, 100]", percent)
	}
	if minInFlight < 1 {
		return nil, fmt.Errorf("retry: share budget minimum %d must be at least 1", minInFlight)
	}
	return &Budget{percent: percent, minInFlight: minInFlight, inflightTotal: inflightTotal}, nil
}

// cap computes the admission ceiling: the fixed number in static
// mode, the traffic share floored at the minimum in share mode.
func (b *Budget) cap() int {
	if b.percent == 0 {
		return b.max
	}
	share := int(int64(b.percent) * b.inflightTotal() / 100)
	return max(b.minInFlight, share)
}

// Acquire admits one retry attempt without blocking and reports whether
// it fit under the cap. A false return means the budget is exhausted
// and the caller must fail fast.
func (b *Budget) Acquire() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.inFlight >= b.cap() {
		return false
	}
	b.inFlight++
	return true
}

// Release returns one admitted slot. Releases without a matching
// acquire are absorbed, never driving the counter negative.
func (b *Budget) Release() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.inFlight > 0 {
		b.inFlight--
	}
}
