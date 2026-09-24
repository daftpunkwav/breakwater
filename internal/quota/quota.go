/**
 * @file quota
 * @description Quota ledger contracts: the lease model.
 *
 * Responsibilities:
 * - Own the monetary balance ledger and the lease lifecycle
 *   (RESERVED -> SETTLED | EXPIRED | CANCELLED)
 * - Nothing else: time-window throughput protection (RPM/TPM) belongs to
 *   the limiter module; token estimation (prompt estimate + max_tokens
 *   clamp) is computed once by the pipeline layer and passed to both
 * - Atomic reservation scripts, the sweeper and the reconciliation
 *   protocol belong to the implementation
 *
 * Invariants carried by this contract:
 * - I3: no over-draft from concurrency; initial balance = current balance
 *   + consumed - refunded must always reconcile to zero error
 * - I9: every reservation has a lease record; reservations outside the
 *   settled set are reclaimed by the sweeper
 *
 * A balance query member joins when the minimal admin API lands.
 */
package quota

import (
	"context"
	"errors"
	"time"
)

// LeaseState enumerates the lease lifecycle.
type LeaseState string

const (
	// LeaseStateReserved marks a live reservation awaiting settlement.
	LeaseStateReserved LeaseState = "RESERVED"
	// LeaseStateSettled marks a reservation reconciled against actual usage.
	LeaseStateSettled LeaseState = "SETTLED"
	// LeaseStateExpired marks a reservation reclaimed by the sweeper after
	// its holder vanished (e.g. process crashed before settling).
	LeaseStateExpired LeaseState = "EXPIRED"
	// LeaseStateCancelled marks a reservation released without consumption.
	LeaseStateCancelled LeaseState = "CANCELLED"
)

// Lease is one quota reservation identified by a unique ID.
type Lease struct {
	ID       string
	TenantID string
	// Amount is the reserved token budget.
	Amount int64
	State  LeaseState
	// CreatedAt orders leases for sweeper scans and reconciliation views.
	CreatedAt time.Time
}

// ErrInsufficientBalance reports a reservation denied because the tenant
// balance cannot cover the requested amount. Callers map it to HTTP 402.
var ErrInsufficientBalance = errors.New("quota: insufficient balance")

// Ledger is the quota account port. Implementations must be safe for
// concurrent use and must never over-draft a balance.
type Ledger interface {
	// Reserve atomically deducts amount from the tenant balance and
	// records a lease.
	Reserve(ctx context.Context, tenantID string, amount int64) (Lease, error)
	// Settle reconciles a live lease against actual usage: the difference
	// (reserved - used) is refunded when positive and never surcharged
	// when negative.
	//
	// Settling a lease that already reached a terminal state (settled,
	// expired, cancelled) must be a detectable no-op: no double refund,
	// the event surfaced through a counter or log. Long streams can
	// outlive the sweeper TTL, so late settles are expected, not errors.
	Settle(ctx context.Context, leaseID string, usedTokens int64) error
	// Cancel releases a live lease without consumption (e.g. the request
	// was rejected before reaching an upstream). Cancelling a terminal
	// lease follows the same no-op rule as Settle.
	Cancel(ctx context.Context, leaseID string) error
	// Balance reports the tenant's current balance. It joins with the
	// minimal admin API (query quota, view breaker state), never with
	// the request path.
	Balance(ctx context.Context, tenantID string) (int64, error)
}
