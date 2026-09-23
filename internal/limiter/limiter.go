/**
 * @file limiter
 * @description Tenant-level rate limiting contracts: RPM and TPM token buckets.
 *
 * Responsibilities:
 * - Define the decision type and the limiter port shared by both backends
 *   (in-memory for tests and degradation, Redis Lua for the atomic path)
 * - Nothing else: bucket state and scripts belong to the implementations
 *
 * The token bucket is implemented in this project by discipline; third
 * party rate limiting libraries must not be imported (enforced by depguard).
 */
package limiter

import (
	"context"
	"time"
)

// Decision is the outcome of one rate limit check.
type Decision struct {
	// Allowed reports whether the request may proceed.
	Allowed bool
	// RetryAfter is valid only when Allowed is false; it backs the
	// Retry-After response header.
	RetryAfter time.Duration
}

// Limiter enforces request-rate (RPM) and token-throughput (TPM) limits
// per tenant. Tokens are estimated before the upstream call (reserve) and
// corrected after real usage is known (refund); the estimate must clamp
// per-request max_tokens so oversized requests cannot monopolize a tenant
// bucket.
type Limiter interface {
	// Allow consumes one request slot and pre-reserves the estimated
	// token cost. Rejected requests must not reach any upstream.
	Allow(ctx context.Context, tenantID string, tokens int64) (Decision, error)
	// Refund returns previously reserved tokens that went unconsumed.
	Refund(ctx context.Context, tenantID string, tokens int64) error
}
