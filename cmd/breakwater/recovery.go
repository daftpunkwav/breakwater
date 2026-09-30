/**
 * @file recovery
 * @description The active recovery loop: asks the upstreams that were
 * taken out of rotation whether they are back, and restores them when
 * they answer.
 *
 * Responsibilities:
 * - Periodically probe auto-disabled upstreams and lift the auto
 *   disable on a healthy answer (never a manual operator disable)
 * - Drive a synthetic probe through the breaker for breaker-open and
 *   idle half-open upstreams, so recovery does not wait for real
 *   traffic to become the probe
 * - Nothing else: what put an upstream out of rotation lives in the
 *   router switch and the circuit breaker; this loop only asks "are
 *   you back"
 */
package main

import (
	"context"
	"log/slog"
	"time"

	"github.com/daftpunkwav/breakwater/internal/circuit"
	"github.com/daftpunkwav/breakwater/internal/obs"
	"github.com/daftpunkwav/breakwater/internal/router"
	"github.com/daftpunkwav/breakwater/internal/upstream"
)

// startRecovery runs the active recovery loop until ctx ends. A zero
// interval (or no probe-capable upstreams) leaves the gateway with
// passive recovery only. Probe targets are the upstreams whose
// configuration declares a probe_url — an upstream without one cannot
// be asked anything off the request path. Restoring an auto-disabled
// upstream takes `passes` consecutive healthy probes; a single failing
// probe resets the count, so a flapping upstream cannot cycle back in.
// A positive `backoff` spaces an auto-disabled upstream's probes out
// after failures: the first failed probe waits one interval, each
// consecutive failure doubles the wait, capped at `backoff` — a hard-down
// upstream is asked ever more rarely instead of every tick. Zero keeps
// every tick probing (the breaker-ejected path needs no ladder either
// way: the breaker's own cooldown already spaces those probes).
func startRecovery(ctx context.Context, interval, timeout, backoff time.Duration, passes int, breaker circuit.Breaker, routingSwitch *router.Switch, probes map[string]upstream.Upstream, metrics *obs.Metrics, logger *slog.Logger) {
	if interval <= 0 || len(probes) == 0 {
		return
	}
	logger.Info("active recovery probing enabled",
		"interval", interval, "upstreams", len(probes), "restore_passes", passes,
		"failed_probe_backoff", backoff)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		book := probeBook{passes: make(map[string]int), retries: make(map[string]probeRetry)}
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				recoverAutoDisabled(ctx, interval, timeout, backoff, passes, routingSwitch, probes, book, metrics, logger)
				reviveEjected(ctx, timeout, breaker, probes, metrics)
			}
		}
	}()
}

// probeBook is the recovery loop's per-upstream bookkeeping: the
// consecutive healthy passes toward the restore threshold, and the
// failed-probe backoff schedule. Both live only inside the loop
// goroutine; no locks needed.
type probeBook struct {
	passes  map[string]int
	retries map[string]probeRetry
}

// probeRetry is one upstream's failed-probe backoff: consecutive
// failures so far, and the earliest next attempt.
type probeRetry struct {
	fails int
	next  time.Time
}

// due reports whether the next attempt is scheduled at or before now.
func (r probeRetry) due(now time.Time) bool { return !r.next.After(now) }

// probeBackoff doubles the base interval per consecutive failure: the
// first failure waits one base interval, each later one twice the
// previous wait, never past the ceiling. The exponent is read from the
// failure count before any doubling, so the ladder never skips a rung
// and the wait stays bounded no matter how long the failure streak
// grows.
func probeBackoff(base, ceiling time.Duration, fails int) time.Duration {
	if ceiling < base {
		base = ceiling
	}
	delay := base
	for i := 1; i < fails; i++ {
		if delay >= ceiling {
			return ceiling
		}
		doubled := delay * 2
		if doubled > ceiling || doubled <= 0 {
			return ceiling
		}
		delay = doubled
	}
	return delay
}

// recoverAutoDisabled probes every due auto-disabled upstream; `passes`
// consecutive healthy answers lift the auto disable, one failing
// answer resets the count. A manual operator disable stays untouched —
// recovery only ever lifts the system's own decision.
func recoverAutoDisabled(ctx context.Context, interval, timeout, backoff time.Duration, passes int, routingSwitch *router.Switch, probes map[string]upstream.Upstream, book probeBook, metrics *obs.Metrics, logger *slog.Logger) {
	now := time.Now()
	for _, id := range routingSwitch.AutoDisabledIDs() {
		target, ok := probes[id]
		if !ok {
			continue
		}
		if r, tracked := book.retries[id]; backoff > 0 && tracked && !r.due(now) {
			continue
		}
		pctx, cancel := context.WithTimeout(ctx, timeout)
		err := target.Probe(pctx)
		cancel()
		metrics.UpstreamProbe(id, err == nil)
		if err != nil {
			book.passes[id] = 0
			// The ladder grows with consecutive failures and a healthy
			// answer forgets it entirely: the read-then-increment order
			// makes the first failure wait one base interval, not two.
			r := book.retries[id]
			r.fails++
			r.next = now.Add(probeBackoff(interval, backoff, r.fails))
			book.retries[id] = r
			continue
		}
		delete(book.retries, id)
		book.passes[id]++
		if book.passes[id] < passes {
			continue
		}
		if err := routingSwitch.AutoEnableUpstream(id); err != nil {
			// The id came from AutoDisabledIDs, which only holds entries
			// AutoDisableUpstream accepted against the known set, so the
			// error is unreachable through this path. The port can still
			// reject; stay defensive.
			continue
		}
		delete(book.passes, id)
		logger.Info("upstream recovered, auto disable lifted", "upstream", id)
	}
}

// reviveEjected offers breaker-ejected upstreams an active probe. The
// probe rides the breaker's own machine — allowed exactly when a real
// request would be — so a healthy answer closes the circuit like a
// served request would, and an unhealthy one re-opens it.
func reviveEjected(ctx context.Context, timeout time.Duration, breaker circuit.Breaker, probes map[string]upstream.Upstream, metrics *obs.Metrics) {
	for id, target := range probes {
		state := breaker.StateOf(ctx, id)
		if state != circuit.StateOpen && state != circuit.StateHalfOpen {
			continue
		}
		ran, err := circuit.ActiveProbe(ctx, breaker, id, timeout, target.Probe)
		if !ran {
			// Cooldown still outstanding, or a real request owns the
			// half-open probe slot: leave the machine alone.
			continue
		}
		metrics.UpstreamProbe(id, err == nil)
	}
}
