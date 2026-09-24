/**
 * @file limiter
 * @description Tenant-level rate limiting contracts: RPM and TPM token buckets.
 *
 * Responsibilities:
 * - Own time-window throughput protection: request-rate (RPM) and
 *   token-throughput (TPM) buckets per tenant
 * - Nothing else: the monetary balance ledger belongs to the quota
 *   module; token estimation (prompt estimate + max_tokens clamp) is
 *   computed once by the pipeline layer and passed to both
 * - Bucket state and scripts belong to the implementations (in-memory
 *   for tests and degradation, Redis Lua for the atomic path)
 *
 * The token bucket is implemented in this project by discipline; third
 * party rate limiting libraries must not be imported (enforced by
 * depguard).
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
	// RetryAfter backs the Retry-After response header. It is valid only
	// when Allowed is false and the call returned no error.
	RetryAfter time.Duration
}

// Limits are the tenant's ceilings for one check. The limiter is a pure
// mechanism: it knows tenants only as key material, never as identity —
// the pipeline resolves the values from the tenant's tier and passes
// them in. Zero disables a ceiling.
type Limits struct {
	// RPM is the per-minute request ceiling.
	RPM int64
	// TPM is the per-minute token ceiling.
	TPM int64
}

// Limiter enforces RPM and TPM limits per tenant. Tokens are estimated
// before the upstream call (reserve) and corrected after real usage is
// known (refund); the estimate must clamp per-request max_tokens so
// oversized requests cannot monopolize a tenant bucket.
type Limiter interface {
	// Allow consumes one request slot and pre-reserves the estimated
	// token cost. Rejected requests must not reach any upstream.
	//
	// When it returns a non-nil error the Decision is undefined and must
	// be ignored; the degradation policy for backend unavailability
	// (fail-closed vs fail-open) is a pipeline-layer policy decision,
	// not encoded here.
	Allow(ctx context.Context, tenantID string, limits Limits, tokens int64) (Decision, error)
	// Refund returns previously reserved tokens that went unconsumed.
	Refund(ctx context.Context, tenantID string, limits Limits, tokens int64) error
}
