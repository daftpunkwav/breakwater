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
 * Contract points:
 * - closed -> open on sustained failures; open fails fast without
 *   touching the upstream
 * - half-open admits exactly one outstanding probe; concurrent arrivals
 *   are denied instead of queueing
 * - a granted call reports its outcome exactly once through the returned
 *   permission; a probe whose holder never reports (panicked, cancelled
 *   or hung) leaves the slot occupied until the next Allow, StateOf or
 *   Report observes the expired probe deadline and reclaims it as a
 *   server fault. Recovery is deadline-based, so a probe still running
 *   past the probe timeout is indistinguishable from an abandoned one,
 *   and a caller that never reports costs the slot its timeout — not
 *   any special handling of its own.
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
	// OutcomeSlow marks a call that completed healthily but took longer
	// than the caller's slow-call threshold to prove itself (time to
	// first byte for streams, full duration otherwise). It is health
	// evidence like Success — the upstream answered — and every
	// strategy without a notion of slowness treats it exactly as
	// Success; the slow-call strategy additionally counts it against
	// the window's slow share.
	OutcomeSlow
	// OutcomeGatewayTerminated marks a call the gateway cut short under
	// its own policy (the stream ceiling). The upstream kept answering
	// until the gateway stopped listening, so the outcome is no health
	// evidence either way: closed keeps the failure counter as it is,
	// and a truncated half-open probe goes back to open with a fresh
	// cooldown — a probe that proves nothing must not close the breaker.
	OutcomeGatewayTerminated
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
	// Reset forces the breaker back to closed, clearing the failure
	// count and any outstanding probe. It is an operator action for
	// "I fixed the upstream, let it through now" — the machine's own
	// cooldown and probe path remain the automatic route back.
	Reset(ctx context.Context, upstreamID string)
}
