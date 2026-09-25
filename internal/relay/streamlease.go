/**
 * @file streamlease
 * @description The context lease of one streaming attempt.
 *
 * Responsibilities:
 * - Split a streaming attempt's time budget: the attempt timeout bounds
 *   time-to-first-byte (headers), the optional stream ceiling bounds the
 *   whole body — a legitimate completion may run for minutes and must
 *   not die at AttemptTimeout
 * - Propagate cancellation to the upstream request the moment either
 *   timer fires or the attempt ends, whichever comes first
 * - Nothing else: the breaker outcome and error classification are the
 *   exchange's business; this file only owns the timers and the context
 *
 * Both timers cancel the derived context; the exchange tells the two
 * firings apart through the fired flags (a time-to-first-byte expiry is
 * a retryable timeout, a ceiling expiry terminates the stream honestly
 * as an upstream timeout).
 */
package relay

import (
	"context"
	"sync/atomic"
	"time"
)

// streamLease is the per-attempt timer set of a streaming exchange.
type streamLease struct {
	cancel       context.CancelFunc
	ttftTimer    *time.Timer
	ceilingTimer *time.Timer
	// The fired flags are stored by the timer goroutines and loaded by
	// the attempt goroutine; atomics keep that hand-off race-free.
	ttftFired    atomic.Bool
	ceilingFired atomic.Bool
}

// beginStream derives the streaming forward context off the request
// context and arms the two timers. The caller owns the lease until it
// calls end.
func (r *run) beginStream() (*streamLease, context.Context) {
	ctx, cancel := context.WithCancel(r.ctx)
	lease := &streamLease{cancel: cancel}
	if d := r.exec.streamTimeout; d > 0 {
		lease.ceilingTimer = time.AfterFunc(d, func() {
			lease.ceilingFired.Store(true)
			cancel()
		})
	}
	if d := r.exec.policy.AttemptTimeout; d > 0 {
		lease.ttftTimer = time.AfterFunc(d, func() {
			lease.ttftFired.Store(true)
			cancel()
		})
	}
	return lease, ctx
}

// ttftPassed stops the time-to-first-byte timer: the reply headers
// arrived, the body may now run as long as the ceiling allows.
func (l *streamLease) ttftPassed() {
	if l.ttftTimer != nil {
		l.ttftTimer.Stop()
	}
}

// end releases the lease at every attempt exit; safe to call twice.
func (l *streamLease) end() {
	if l.ttftTimer != nil {
		l.ttftTimer.Stop()
		l.ttftTimer = nil
	}
	if l.ceilingTimer != nil {
		l.ceilingTimer.Stop()
		l.ceilingTimer = nil
	}
	l.cancel()
}
