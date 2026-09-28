/**
 * @file adminassembly
 * @description Assembly of the admin surface's live bindings: which
 * backend each admin endpoint reads and acts on.
 *
 * Responsibilities:
 * - Bind the balance endpoints to the live ledger, the state and reset
 *   views to the breaker registry, and the on-demand probe to the
 *   probe-capable upstream subset
 * - Nothing else: the admin surface's routes and payloads live in the
 *   server package; this file only connects them to concrete resources
 */
package main

import (
	"context"
	"net/http"

	"github.com/daftpunkwav/breakwater/internal/circuit"
	"github.com/daftpunkwav/breakwater/internal/config"
	"github.com/daftpunkwav/breakwater/internal/obs"
	"github.com/daftpunkwav/breakwater/internal/server"
	"github.com/daftpunkwav/breakwater/internal/upstream"
)

// adminBindings names the live backends the admin surface reads and
// acts on: the ledger behind the balance endpoints, the breaker
// registry behind the state and reset views, the configured upstream
// list and adapters that decide what is known, and the probe-capable
// subset behind the on-demand probe.
type adminBindings struct {
	gov         *governanceBackends
	breaker     circuit.Breaker
	metrics     *obs.Metrics
	upstreamIDs []string
	adapters    map[string]upstream.Upstream
	probes      map[string]upstream.Upstream
}

// buildAdmin binds the admin endpoints to the live backends; options
// forward to server.NewAdmin (the routing switches, the identity
// administration store). The breaker-reset and on-demand probe
// actions are bound here too: reset consults the configured upstream
// set, the probe consults the probe-capable subset and counts on the
// same metric the recovery loop records.
func buildAdmin(cfg config.Config, b adminBindings, opts ...server.AdminOption) http.Handler {
	balances := func(r *http.Request, tenantID string) (int64, error) {
		return b.gov.ledger.Balance(r.Context(), tenantID)
	}
	setBalance := func(r *http.Request, tenantID string, balance int64) error {
		return b.gov.ledger.SetBalance(r.Context(), tenantID, balance)
	}
	states := func(r *http.Request) []server.BreakerView {
		views := make([]server.BreakerView, 0, len(b.upstreamIDs))
		for _, id := range b.upstreamIDs {
			views = append(views, server.BreakerView{
				Upstream: id,
				State:    b.breaker.StateOf(r.Context(), id),
			})
		}
		return views
	}
	reset := func(r *http.Request, id string) error {
		if _, ok := b.adapters[id]; !ok {
			return server.ErrUnknownUpstream
		}
		b.breaker.Reset(r.Context(), id)
		return nil
	}
	probe := func(r *http.Request, id string) error {
		target, ok := b.probes[id]
		if !ok {
			if _, known := b.adapters[id]; known {
				return server.ErrProbeUnconfigured
			}
			return server.ErrUnknownUpstream
		}
		// A zero timeout means "no ceiling", matching the
		// circuit.ActiveProbe convention: with the recovery loop
		// disabled the operator may legitimately leave the knob unset,
		// and WithTimeout(ctx, 0) would expire before the probe ran.
		ctx := r.Context()
		if cfg.Probe.Timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, cfg.Probe.Timeout)
			defer cancel()
		}
		err := target.Probe(ctx)
		b.metrics.UpstreamProbe(id, err == nil)
		return err
	}
	opts = append(opts, server.WithBreakerReset(reset), server.WithUpstreamProbe(probe))
	return server.NewAdmin(cfg.Security.AdminToken, balances, setBalance, states, opts...)
}

// upstreamIDs lists the configured upstream identifiers in order.
func upstreamIDs(cfgs []config.Upstream) []string {
	ids := make([]string, 0, len(cfgs))
	for _, c := range cfgs {
		ids = append(ids, c.ID)
	}
	return ids
}
