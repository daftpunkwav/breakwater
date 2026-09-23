/**
 * @file retry
 * @description Retry protocol contracts: attempt layering and budgets.
 *
 * Responsibilities:
 * - Define the policy shape, the retryability boundary and the global
 *   budget port
 * - Nothing else: backoff math, error classification tables and the
 *   attempt loop belong to the implementation
 *
 * Contract points:
 * - I5: a request never exceeds its attempt cap, and retries never
 *   exceed the global in-flight budget (retry storm containment)
 * - I6: no transparent retry or replay after the first response byte has
 *   reached the client; mid-stream failures terminate via the SSE error
 *   event contract
 */
package retry

import (
	"context"
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

// Classifier reports whether an error may be retried on a fresh attempt,
// possibly on another upstream. Connection failures, 429 and 5xx are
// retryable; client errors (4xx) and post-first-byte stream failures
// are not.
type Classifier interface {
	Retryable(err error) bool
}

// Budget is the process-wide in-flight retry budget shared by all
// requests; it bounds how much of total traffic is retry amplification.
type Budget interface {
	// Acquire admits one retry attempt and reports false when the global
	// budget is exhausted; the caller must then fail fast.
	Acquire(ctx context.Context) bool
	// Release returns one acquired slot.
	Release()
}
