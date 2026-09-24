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
	"net"
	"net/http"
	"net/url"
)

// StatusError marks one completed upstream exchange that ended in an
// error status code. The body of such exchanges is drained by whoever
// builds the error; only the status survives for classification.
type StatusError struct {
	StatusCode int
}

// NewStatusError wraps an upstream error status for classification.
func NewStatusError(statusCode int) *StatusError {
	return &StatusError{StatusCode: statusCode}
}

// Error implements error.
func (e *StatusError) Error() string {
	return fmt.Sprintf("upstream exchange failed with status %d", e.StatusCode)
}

// ErrCommitted marks a failure that happened after the first response
// byte reached the client. The attempt loop returns it without
// consulting the classifier (invariant I6): a retry would replay a
// half-sent reply.
var ErrCommitted = errors.New("retry: attempt already committed bytes to the client")

// DefaultClassifier is the standard retryability table described in the
// file header.
type DefaultClassifier struct{}

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
	var urlErr *url.Error
	if errors.As(err, &urlErr) {
		if urlErr.Timeout() {
			return true
		}
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return netErr.Timeout()
	}
	// Remaining transport failures (connection refused, reset, broken
	// pipe) are transient by nature.
	return true
}
