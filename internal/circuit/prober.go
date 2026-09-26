/**
 * @file prober
 * @description Active probing through the breaker machine: a helper
 * that turns one synthetic health check into a regular breaker
 * attempt, so recovery does not depend on real traffic arriving.
 *
 * Responsibilities:
 * - Run one probe function under the breaker's Allow/Report protocol
 * - Nothing else: which upstreams to probe and on what schedule is the
 *   caller's policy (assembly); the port itself stays unchanged
 *
 * The probe rides the same half-open rules as a real request: an open
 * breaker whose cooldown has not elapsed denies the probe, a cooldown-
 * elapsed one admits it as THE half-open probe (concurrent real
 * requests are then denied until it reports), and the outcome moves
 * the machine exactly like a served request would.
 */
package circuit

import (
	"context"
	"errors"
	"time"
)

// ActiveProbe runs one health check through the breaker. It returns
// false when the breaker denied the attempt (open cooldown outstanding,
// or another probe already owns the half-open slot) — the probe was not
// run at all. A granted probe reports its outcome exactly once: a nil
// probe error is a success, a failed probe is a server fault, and a
// probe cancelled by the caller's own context is a client fault that
// must not advance the failure count.
func ActiveProbe(ctx context.Context, b Breaker, upstreamID string, timeout time.Duration, probe func(context.Context) error) (ran bool, err error) {
	perm, ok := b.Allow(ctx, upstreamID)
	if !ok {
		return false, nil
	}
	pctx := ctx
	var cancel context.CancelFunc
	if timeout > 0 {
		pctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	perr := probe(pctx)
	switch {
	case perr == nil:
		perm.Report(OutcomeSuccess)
	case ctx.Err() != nil && errors.Is(perr, context.Canceled):
		// The prober was shut down mid-check: nobody's verdict on the
		// upstream, same rule the relay applies to client disconnects.
		perm.Report(OutcomeClientFault)
	default:
		perm.Report(OutcomeServerFault)
	}
	return true, perr
}
