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
 * - half-open admits exactly one probe; concurrent arrivals fail fast
 *   instead of queueing
 * - state is process-local; the port stays replaceable so a shared
 *   backend can be introduced without touching callers
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

// Breaker guards calls against one dependency identity (an upstream, or a
// model on an upstream when granularity is refined).
type Breaker interface {
	// Allow reports whether a call to the upstream may proceed now.
	Allow(ctx context.Context, upstreamID string) bool
	// Record feeds the outcome of a granted call back into the machine.
	Record(ctx context.Context, upstreamID string, success bool)
	// StateOf exposes the current state for metrics and the admin API.
	StateOf(ctx context.Context, upstreamID string) State
}
