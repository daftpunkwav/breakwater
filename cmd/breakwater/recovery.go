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
func startRecovery(ctx context.Context, interval, timeout time.Duration, passes int, breaker circuit.Breaker, routingSwitch *router.Switch, probes map[string]upstream.Upstream, metrics *obs.Metrics, logger *slog.Logger) {
	if interval <= 0 || len(probes) == 0 {
		return
	}
	logger.Info("active recovery probing enabled",
		"interval", interval, "upstreams", len(probes), "restore_passes", passes)
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		counts := make(map[string]int)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				recoverAutoDisabled(ctx, timeout, passes, routingSwitch, probes, counts, metrics, logger)
				reviveEjected(ctx, timeout, breaker, probes, metrics)
			}
		}
	}()
}

// recoverAutoDisabled probes every auto-disabled upstream; `passes`
// consecutive healthy answers lift the auto disable, one failing
// answer resets the count. A manual operator disable stays untouched —
// recovery only ever lifts the system's own decision.
func recoverAutoDisabled(ctx context.Context, timeout time.Duration, passes int, routingSwitch *router.Switch, probes map[string]upstream.Upstream, counts map[string]int, metrics *obs.Metrics, logger *slog.Logger) {
	for _, id := range routingSwitch.AutoDisabledIDs() {
		target, ok := probes[id]
		if !ok {
			continue
		}
		pctx, cancel := context.WithTimeout(ctx, timeout)
		err := target.Probe(pctx)
		cancel()
		metrics.UpstreamProbe(id, err == nil)
		if err != nil {
			counts[id] = 0
			continue
		}
		counts[id]++
		if counts[id] < passes {
			continue
		}
		if err := routingSwitch.AutoEnableUpstream(id); err != nil {
			// The id came from AutoDisabledIDs, which only holds entries
			// AutoDisableUpstream accepted against the known set, so the
			// error is unreachable through this path. The port can still
			// reject; stay defensive.
			continue
		}
		delete(counts, id)
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
