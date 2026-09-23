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
 * - I5: a request never exceeds its attempt cap, and retries never
 *   exceed the global in-flight budget (retry storm containment)
 * - I6: the attempt loop — not the classifier — owns the first-byte
 *   boundary. After the first response byte has reached the client the
 *   loop must not consult the classifier at all; such failures
 *   terminate via the SSE error event contract. Emitting that event
 *   belongs to the forward stage that owns the client response;
 *   deciding that no attempt remains belongs here.
 */
package retry

import (
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
// The classifier sees errors only. Positional gating (invariant I6:
// nothing is retryable after the first response byte) is enforced by the
// attempt loop before the classifier is ever consulted.
type Classifier interface {
	Retryable(err error) bool
}

// Budget is the process-wide in-flight retry budget shared by all
// requests; it bounds how much of total traffic is retry amplification
// (retry storm containment, I5). It is a concrete type on purpose: a
// single in-process counter with no foreseeable alternative backend,
// unlike the breaker whose backend stays replaceable.
type Budget struct {
	mu       sync.Mutex
	inFlight int
	max      int
}

// NewBudget builds a budget admitting at most maxInFlight concurrent
// retry attempts.
func NewBudget(maxInFlight int) *Budget {
	if maxInFlight < 0 {
		maxInFlight = 0
	}
	return &Budget{max: maxInFlight}
}

// Acquire admits one retry attempt without blocking and reports whether
// it fit under the cap. A false return means the budget is exhausted
// and the caller must fail fast.
func (b *Budget) Acquire() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.inFlight >= b.max {
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
