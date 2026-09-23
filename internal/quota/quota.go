/**
 * @file quota
 * @description Quota ledger contracts: the lease model.
 *
 * Responsibilities:
 * - Define the lease lifecycle (RESERVED -> SETTLED | EXPIRED | CANCELLED)
 *   and the ledger port
 * - Nothing else: atomic reservation scripts, the sweeper and the
 *   reconciliation protocol belong to the implementation
 *
 * Invariants carried by this contract:
 * - I3: no over-draft from concurrency; initial balance = current balance
 *   + consumed - refunded must always reconcile to zero error
 * - I9: every reservation has a lease record; reservations outside the
 *   settled set are reclaimed by the sweeper
 */
package quota

import (
	"context"
	"errors"
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
	Settle(ctx context.Context, leaseID string, usedTokens int64) error
	// Cancel releases a live lease without consumption (e.g. the request
	// was rejected before reaching an upstream).
	Cancel(ctx context.Context, leaseID string) error
}
