/**
 * @file circuit
 * @description Circuit breaker contracts: per-upstream three-state machine.
 *
 * Responsibilities:
 * - Define the state vocabulary and the guard port shared by callers and
 *   the admin API
 * - Nothing else: thresholds, cooldowns and probe scheduling belong to
 *   the implementation
 *
 * Contract points (invariant I4):
 * - closed -> open on sustained failures; open fails fast without
 *   touching the upstream
 * - half-open admits exactly one outstanding probe; concurrent arrivals
 *   are denied instead of queueing
 * - a granted call reports its outcome exactly once through the returned
 *   permission; implementations must reclaim abandoned permissions (lost
 *   to panic, cancellation or probe timeout) so the half-open slot
 *   cannot leak — the guarantee is structural, not caller discipline
 * - state is process-local; the port stays replaceable so a shared
 *   backend can be introduced without touching callers
 *
 * Which upstream answers count as failures is the caller's policy; the
 * port only distinguishes the accounting categories defined on Outcome.
 */
package circuit

import "context"

// State enumerates the breaker states.
type State string

const (
	// StateClosed lets traffic through while failures stay under threshold.
	StateClosed State = "closed"
	// StateOpen rejects every call immediately.
	StateOpen State = "open"
	// StateHalfOpen admits a single probe after the cooldown elapses.
	StateHalfOpen State = "half-open"
)

// Outcome classifies the result of a granted call for breaker accounting.
type Outcome int

const (
	// OutcomeSuccess marks a completed, healthy exchange.
	OutcomeSuccess Outcome = iota
	// OutcomeClientFault marks a rejection caused by the request itself
	// (upstream 4xx); the upstream is healthy and the failure counter
	// must not advance.
	OutcomeClientFault
	// OutcomeServerFault marks an upstream-side failure (5xx, timeout,
	// connection reset).
	OutcomeServerFault
)

// Permission is one granted call slot. The holder reports the outcome of
// the call exactly once; double reports and abandoned permissions are
// absorbed by the implementation.
type Permission interface {
	// Report feeds the outcome into the breaker state machine.
	Report(outcome Outcome)
}

// Breaker guards calls against one dependency identity (an upstream, or
// a model on an upstream when granularity is refined).
type Breaker interface {
	// Allow grants one call slot. The second return value reports
	// whether the call may proceed; when it is false the upstream must
	// not be touched and the returned permission is nil.
	Allow(ctx context.Context, upstreamID string) (Permission, bool)
	// StateOf exposes the current state for metrics and the admin API.
	StateOf(ctx context.Context, upstreamID string) State
}
