/**
 * @file classifier
 * @description The standard retryability table: which attempt failures
 * deserve another attempt.
 *
 * Responsibilities:
 * - Classify one attempt error as retryable or terminal
 * - Nothing else: the attempt loop owns the first-byte boundary and the
 *   budgets; this table only answers the classification question
 *
 * The table reads two error shapes:
 * - StatusError: one completed upstream exchange that ended in an error
 *   status; 429 and 5xx are retryable, other 4xx are the client's fault
 * - everything else: transport-level failures; timeouts are retryable,
 *   client-side cancellation is not (the caller is gone)
 */
package retry

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// StatusError marks one completed upstream exchange that ended in an
// error status code. The body of such exchanges is drained by whoever
// builds the error; only the status survives for classification.
type StatusError struct {
	StatusCode int
	// RetryAfter is the wait the upstream itself requested (its
	// Retry-After header), already parsed and capped. The attempt loop
	// prefers it over the computed backoff for the next attempt against
	// the same upstream; zero means the upstream asked for nothing.
	RetryAfter time.Duration
}

// NewStatusError wraps an upstream error status for classification.
func NewStatusError(statusCode int) *StatusError {
	return &StatusError{StatusCode: statusCode}
}

// Error implements error.
func (e *StatusError) Error() string {
	return fmt.Sprintf("upstream exchange failed with status %d", e.StatusCode)
}

// StripRetryAfter clears any Retry-After hint from err in place and
// returns err itself. The in-place clear is deliberate: the relay's
// passthrough renderer matches the final loop error against the stash
// by object identity, and the loop wraps that error when an overall
// deadline or the retry budget ends the run — a clone would survive
// inside the wrap yet fail the identity match, downgrading an upstream
// error passthrough to a generic gateway envelope. The hint must die
// either way: no later attempt may wait on a schedule a different
// upstream or credential asked for. Errors without a hint pass through
// untouched.
func StripRetryAfter(err error) error {
	var status *StatusError
	if errors.As(err, &status) && status.RetryAfter > 0 {
		status.RetryAfter = 0
	}
	return err
}

// ErrCommitted marks a failure that happened after the first response
// byte reached the client. The attempt loop returns it without
// consulting the classifier: a retry would replay a
// half-sent reply.
var ErrCommitted = errors.New("retry: attempt already committed bytes to the client")

// retryAfterCap bounds a parsed Retry-After hint. The upstream's word
// is authoritative but not unlimited; the overall deadline remains the
// hard bound regardless.
const retryAfterCap = 60 * time.Second

// ParseRetryAfter parses one Retry-After header value: an integer or
// fractional second count, or an HTTP-date. Zero reports "no usable
// hint" — absent, malformed, already elapsed, or capped values included.
func ParseRetryAfter(raw string, now time.Time) time.Duration {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}
	// The magnitude check happens in the numeric domain: building the
	// Duration first would overflow for absurd counts and land on an
	// arbitrary sign instead of the cap.
	if secs, err := strconv.ParseInt(raw, 10, 64); err == nil {
		if secs > int64(retryAfterCap/time.Second) {
			return retryAfterCap
		}
		return cappedRetryAfter(time.Duration(secs) * time.Second)
	}
	if frac, err := strconv.ParseFloat(raw, 64); err == nil {
		// ParseFloat also accepts NaN and infinities. A NaN carries no
		// usable wait and folds to "no hint"; any finite value past the
		// cap collapses to the cap in the numeric domain.
		if math.IsNaN(frac) {
			return 0
		}
		if frac > float64(retryAfterCap/time.Second) {
			return retryAfterCap
		}
		return cappedRetryAfter(time.Duration(frac * float64(time.Second)))
	}
	if t, err := http.ParseTime(raw); err == nil {
		return cappedRetryAfter(t.Sub(now))
	}
	return 0
}

// cappedRetryAfter sanitizes one parsed wait: negative and zero waits
// mean "no hint", everything above the cap collapses to the cap.
func cappedRetryAfter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	return min(d, retryAfterCap)
}

// DefaultClassifier is the standard retryability table described in the
// file header.
type DefaultClassifier struct{}

// DelayHint reports the wait the upstream itself requested before the
// next attempt, when the error carries a Retry-After hint. The attempt
// loop prefers this over its computed backoff. Errors without a hint
// yield zero and the loop falls back to its own schedule.
func (DefaultClassifier) DelayHint(err error) time.Duration {
	var status *StatusError
	if errors.As(err, &status) {
		return status.RetryAfter
	}
	return 0
}

// Retryable implements Classifier.
func (DefaultClassifier) Retryable(err error) bool {
	if err == nil {
		return false
	}
	// The client went away: no amount of retries serves it.
	if errors.Is(err, context.Canceled) {
		return false
	}
	// Timeouts are the canonical transient fault.
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var status *StatusError
	if errors.As(err, &status) {
		return status.StatusCode == http.StatusTooManyRequests || status.StatusCode >= 500
	}
	// Everything left is a transport-level failure. net/http wraps all
	// of them in *url.Error, so a refused connection, a reset and a
	// broken pipe land here rather than in a timeout-specific branch.
	// For the attempt loop they are as transient as a timeout: the
	// fault belongs to one candidate, and the next candidate may well
	// be a different host that answers. Treating them as terminal would
	// turn a single dead upstream into a failed request instead of a
	// failover.
	return true
}
