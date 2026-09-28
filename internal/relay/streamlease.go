/**
 * @file streamlease
 * @description The context lease of one streaming attempt.
 *
 * Responsibilities:
 * - Split a streaming attempt's time budget: the attempt timeout bounds
 *   time-to-first-byte (headers), the optional stream ceiling bounds
 *   the whole body — a legitimate completion may run for minutes and
 *   must not die at AttemptTimeout — and the optional idle watchdog
 *   bounds upstream silence: a body that stops producing is cut even
 *   while its total budget lasts
 * - Propagate cancellation to the upstream request the moment any
 *   timer fires or the attempt ends, whichever comes first
 * - Nothing else: the breaker outcome and error classification are the
 *   exchange's business; this file only owns the timers and the context
 *
 * Every timer cancels the derived context; the exchange tells the
 * firings apart through the fired flags (a time-to-first-byte expiry
 * is a retryable timeout, a ceiling or idle expiry terminates the
 * stream honestly as an upstream timeout).
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
	// The idle watchdog is armed at the commit point and re-armed on
	// every body read: a stream whose upstream goes silent for the
	// whole window is cut however young the body is. Zero disables it.
	idleTimer    *time.Timer
	idleDuration time.Duration
	// The fired flags are stored by the timer goroutines and loaded by
	// the attempt goroutine; atomics keep that hand-off race-free.
	ttftFired    atomic.Bool
	ceilingFired atomic.Bool
	idleFired    atomic.Bool
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

// armIdle starts the idle watchdog once the reply is committed: from
// here on, upstream silence for the whole window is a broken stream
// however much time the total budget would still allow.
func (l *streamLease) armIdle(d time.Duration) {
	if d <= 0 {
		return
	}
	l.idleDuration = d
	l.idleTimer = time.AfterFunc(d, func() {
		l.idleFired.Store(true)
		l.cancel()
	})
}

// activity records upstream progress: every body read re-arms the idle
// watchdog. Resetting a fired timer is harmless — the fired flag is
// sticky and the exchange reads it only after the pump has died.
func (l *streamLease) activity() {
	if l.idleTimer != nil {
		l.idleTimer.Reset(l.idleDuration)
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
	if l.idleTimer != nil {
		l.idleTimer.Stop()
		l.idleTimer = nil
	}
	l.cancel()
}
