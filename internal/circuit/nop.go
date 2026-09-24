/**
 * @file nop
 * @description The breaker-less implementation: a Breaker that grants
 * everything and forgets every outcome.
 *
 * Responsibilities:
 * - Keep the breaker port optional at assembly time: deployments that
 *   run without circuit breaking compose this instead of branching on
 *   nil at every call site
 * - Nothing else: it holds no state and answers no questions
 */
package circuit

import "context"

// NopBreaker grants every call unconditionally.
type NopBreaker struct{}

// Allow implements Breaker: always granted.
func (NopBreaker) Allow(context.Context, string) (Permission, bool) {
	return nopPermission{}, true
}

// StateOf implements Breaker: the nop never opens.
func (NopBreaker) StateOf(context.Context, string) State { return StateClosed }

// nopPermission absorbs outcome reports.
type nopPermission struct{}

// Report implements Permission: a no-op.
func (nopPermission) Report(Outcome) {}
