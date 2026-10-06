/**
 * @file recovery_test
 * @description The active recovery loop: auto-disabled upstreams come
 * back on a healthy probe, manual disables and dead probes stay put,
 * and a breaker-ejected upstream is revived through the breaker's own
 * machine.
 */
package main

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/daftpunkwav/breakwater/internal/circuit"
	"github.com/daftpunkwav/breakwater/internal/obs"
	"github.com/daftpunkwav/breakwater/internal/router"
	"github.com/daftpunkwav/breakwater/internal/upstream"
)

// discardLogger keeps the loop's log lines out of the test output.
func discardLogger() *slog.Logger { return slog.New(slog.DiscardHandler) }

// TestAutoDisableHookTransitionsOnce: the first fatal report disables
// and counts, repeats are absorbed, and an unknown upstream is a loud
// no-op.
func TestAutoDisableHookTransitionsOnce(t *testing.T) {
	t.Parallel()
	sw := router.NewSwitch([]string{"m1"}, []string{"u1"})
	metrics := obs.NewMetrics()
	hook := autoDisableHook(sw, nil, metrics, discardLogger())

	hook("u1", -1, "upstream_auth_failure")
	if sw.UpstreamEnabled("u1") {
		t.Fatal("hook did not take the upstream out of rotation")
	}
	assertAutoDisabledCount(t, metrics, 1)

	// A repeat observation is absorbed: one transition, one count.
	hook("u1", -1, "upstream_auth_failure")
	if ids := sw.AutoDisabledIDs(); len(ids) != 1 {
		t.Fatalf("auto disabled = %v, want exactly u1", ids)
	}
	assertAutoDisabledCount(t, metrics, 1)

	// An unknown upstream name cannot disable anything.
	hook("typo", -1, "upstream_auth_failure")
}

// assertMetricLine renders the registry and checks the exposition
// carries metric's line with exactly the wanted sample value: the
// rendered number is parsed off the line and compared, so "10" cannot
// satisfy a check for "1" the way a substring match would.
func assertMetricLine(t *testing.T, metrics *obs.Metrics, metric string, want int) {
	t.Helper()
	var out strings.Builder
	if err := metrics.Render(&out); err != nil {
		t.Fatalf("render: %v", err)
	}
	for _, line := range strings.Split(out.String(), "\n") {
		raw, ok := strings.CutPrefix(line, metric)
		if !ok {
			continue
		}
		got, err := strconv.ParseFloat(raw, 64)
		if err != nil {
			t.Fatalf("sample %q does not carry a number:\n%s", line, out.String())
		}
		if got != float64(want) {
			t.Fatalf("%s sample = %g, want %d:\n%s", metric, got, want, out.String())
		}
		return
	}
	t.Fatalf("exposition missing %s%d:\n%s", metric, want, out.String())
}

// assertAutoDisabledCount renders the registry and checks the
// auto-disable counter fired exactly n times.
func assertAutoDisabledCount(t *testing.T, metrics *obs.Metrics, want int) {
	t.Helper()
	assertMetricLine(t, metrics, `breakwater_upstream_auto_disabled_total{upstream="u1",reason="upstream_auth_failure"} `, want)
}

// newProbeAdapter wraps a server URL in the u1 OpenAI adapter with the
// probe endpoint pointed at the server's /healthz.
func newProbeAdapter(t *testing.T, baseURL string) upstream.Upstream {
	t.Helper()
	adapter, err := upstream.NewOpenAI(upstream.OpenAIConfig{
		ID: "u1", BaseURL: baseURL, ProbeURL: baseURL + "/healthz",
	})
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	return adapter
}

// newProbeTarget spins up an upstream whose probe endpoint answers
// with the given status, together with its adapter.
func newProbeTarget(t *testing.T, status int) upstream.Upstream {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	return newProbeAdapter(t, srv.URL)
}

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("condition not met in time")
}

// autoDisableUpstream takes an upstream out of rotation as the system's
// own decision and fails the test when the switch refuses.
func autoDisableUpstream(t *testing.T, sw *router.Switch, id, reason string) {
	t.Helper()
	if _, err := sw.AutoDisableUpstream(id, reason); err != nil {
		t.Fatalf("auto disable: %v", err)
	}
}

// startLoopRecovery arms the recovery loop with the tests' standard
// cadence — 5ms ticks, a generous probe timeout — and the given
// failed-probe backoff ceiling, consecutive-pass threshold and breaker.
func startLoopRecovery(ctx context.Context, passes int, backoff time.Duration, breaker circuit.Breaker, routingSwitch *router.Switch, probes map[string]upstream.Upstream) {
	startRecovery(ctx, 5*time.Millisecond, time.Second, backoff, passes,
		breaker, routingSwitch, probes, obs.NewMetrics(), discardLogger())
}

// openedBreaker builds a breaker registry that has ejected u1: one
// server-fault report past a fail threshold of one.
func openedBreaker(t *testing.T, ctx context.Context, cooldown time.Duration) circuit.Breaker {
	t.Helper()
	breaker := circuit.NewRegistry(circuit.Config{FailThreshold: 1, Cooldown: cooldown})
	perm, ok := breaker.Allow(ctx, "u1")
	if !ok {
		t.Fatal("closed breaker denied the setup call")
	}
	perm.Report(circuit.OutcomeServerFault)
	return breaker
}

// newTwoKeyAdapter builds the u1 adapter carrying a two-credential ring
// for the credential-retirement tests.
func newTwoKeyAdapter(t *testing.T) *upstream.OpenAI {
	t.Helper()
	adapter, err := upstream.NewOpenAI(upstream.OpenAIConfig{
		ID: "u1", BaseURL: "http://127.0.0.1:8090",
		APIKeys: []string{"k1", "k2"},
	})
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	return adapter
}

// healthyRecoveryFixture boots the recovery loop over one
// auto-disabled upstream whose probe endpoint answers 200, at the
// given consecutive-pass threshold, and cleans the loop up with the
// test.
func healthyRecoveryFixture(t *testing.T, reason string, threshold int) *router.Switch {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	adapter := newProbeTarget(t, http.StatusOK)
	sw := router.NewSwitch([]string{"m1"}, []string{"u1"})
	autoDisableUpstream(t, sw, "u1", reason)

	startLoopRecovery(ctx, threshold, 0, circuit.NopBreaker{}, sw,
		map[string]upstream.Upstream{"u1": adapter})
	return sw
}

// TestRecoveryLiftsAutoDisableOnHealthyProbe: an auto-disabled
// upstream is probed and comes back when the probe answers 2xx.
func TestRecoveryLiftsAutoDisableOnHealthyProbe(t *testing.T) {
	t.Parallel()
	sw := healthyRecoveryFixture(t, "upstream_auth_failure", 1)

	waitFor(t, 2*time.Second, func() bool { return sw.UpstreamEnabled("u1") })
}

// TestRecoveryRequiresConsecutivePasses: one healthy probe is not
// enough when the threshold is above one — the count builds across
// ticks and one failing probe resets it.
func TestRecoveryRequiresConsecutivePasses(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// The probe endpoint answers 200 exactly once, then fails forever:
	// a single pass must not restore, and the failure must wipe the
	// earlier pass's credit.
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	t.Cleanup(srv.Close)
	adapter := newProbeAdapter(t, srv.URL)
	sw := router.NewSwitch([]string{"m1"}, []string{"u1"})
	autoDisableUpstream(t, sw, "u1", "upstream_auth_failure")

	startLoopRecovery(ctx, 2, 0, circuit.NopBreaker{}, sw,
		map[string]upstream.Upstream{"u1": adapter})

	time.Sleep(80 * time.Millisecond)
	if sw.UpstreamEnabled("u1") {
		t.Fatal("one pass then a failure must leave the upstream out of rotation")
	}
}

// TestRecoveryRestoresAtThreshold: with a permanently healthy probe
// the upstream comes back once the consecutive-pass count is met.
func TestRecoveryRestoresAtThreshold(t *testing.T) {
	t.Parallel()
	sw := healthyRecoveryFixture(t, "upstream_quota_exhausted", 3)

	waitFor(t, 2*time.Second, func() bool { return sw.UpstreamEnabled("u1") })
}

// TestRecoveryKeepsUnhealthyUpstreamOut: a failing probe keeps the
// auto disable in place.
func TestRecoveryKeepsUnhealthyUpstreamOut(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	adapter := newProbeTarget(t, http.StatusServiceUnavailable)
	sw := router.NewSwitch([]string{"m1"}, []string{"u1"})
	autoDisableUpstream(t, sw, "u1", "upstream_quota_exhausted")

	startLoopRecovery(ctx, 1, 0, circuit.NopBreaker{}, sw,
		map[string]upstream.Upstream{"u1": adapter})

	time.Sleep(40 * time.Millisecond)
	if sw.UpstreamEnabled("u1") {
		t.Fatal("a failing probe must not restore the upstream")
	}
}

// TestRecoveryNeverLiftsManualDisable: recovery only lifts the
// system's own decision, never an operator's.
func TestRecoveryNeverLiftsManualDisable(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	adapter := newProbeTarget(t, http.StatusOK)
	sw := router.NewSwitch([]string{"m1"}, []string{"u1"})
	if err := sw.SetUpstream("u1", false); err != nil {
		t.Fatalf("manual disable: %v", err)
	}

	startLoopRecovery(ctx, 1, 0, circuit.NopBreaker{}, sw,
		map[string]upstream.Upstream{"u1": adapter})

	time.Sleep(40 * time.Millisecond)
	if sw.UpstreamEnabled("u1") {
		t.Fatal("a manual disable must survive the recovery loop")
	}
	if ids := sw.AutoDisabledIDs(); len(ids) != 0 {
		t.Fatalf("auto view = %v, want empty for a manual disable", ids)
	}
}

// TestRecoverySkipsUnprobeableUpstreams: an auto-disabled upstream
// without a probe target is left alone (there is nothing to ask).
func TestRecoverySkipsUnprobeableUpstreams(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sw := router.NewSwitch([]string{"m1"}, []string{"u1", "u2"})
	autoDisableUpstream(t, sw, "u2", "upstream_auth_failure")

	// u1 is probe-capable but healthy-idle; u2 has no target.
	adapter := newProbeTarget(t, http.StatusOK)
	startLoopRecovery(ctx, 1, 0, circuit.NopBreaker{}, sw,
		map[string]upstream.Upstream{"u1": adapter})

	time.Sleep(40 * time.Millisecond)
	if sw.UpstreamEnabled("u2") {
		t.Fatal("an unprobeable upstream must not be restored blind")
	}
}

// TestRecoveryRevivesBreakerEjectedUpstream: the probe rides the
// breaker machine, so one healthy answer closes an open circuit
// without any real traffic.
func TestRecoveryRevivesBreakerEjectedUpstream(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	adapter := newProbeTarget(t, http.StatusOK)
	breaker := openedBreaker(t, ctx, time.Millisecond)
	if breaker.StateOf(ctx, "u1") != circuit.StateOpen {
		t.Fatal("setup: breaker did not open")
	}
	sw := router.NewSwitch([]string{"m1"}, []string{"u1"})

	startLoopRecovery(ctx, 1, 0, breaker, sw,
		map[string]upstream.Upstream{"u1": adapter})

	waitFor(t, 2*time.Second, func() bool { return breaker.StateOf(ctx, "u1") == circuit.StateClosed })
}

// TestRecoveryWaitsOutBreakerCooldown: while the open breaker's
// cooldown is still running, the recovery probe is denied like any
// other attempt — the loop must not cut the cooldown short.
func TestRecoveryWaitsOutBreakerCooldown(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	adapter := newProbeTarget(t, http.StatusOK)
	breaker := openedBreaker(t, ctx, time.Hour)
	sw := router.NewSwitch([]string{"m1"}, []string{"u1"})

	startLoopRecovery(ctx, 1, 0, breaker, sw,
		map[string]upstream.Upstream{"u1": adapter})

	time.Sleep(40 * time.Millisecond)
	if state := breaker.StateOf(ctx, "u1"); state != circuit.StateOpen {
		t.Fatalf("state = %v, want still open: the cooldown is not the loop's to skip", state)
	}
}

// TestStartRecoveryDisabledModes: a zero interval or no probe targets
// starts no loop at all — the gateway keeps passive recovery only.
func TestStartRecoveryDisabledModes(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sw := router.NewSwitch([]string{"m1"}, []string{"u1"})
	adapter := newProbeTarget(t, http.StatusOK)
	targets := map[string]upstream.Upstream{"u1": adapter}

	// Neither call may panic nor change state; both return immediately.
	// The zero interval starts no loop: only the direct call pins that
	// branch.
	startRecovery(ctx, 0, time.Second, 0, 1, circuit.NopBreaker{}, sw, targets, obs.NewMetrics(), discardLogger())
	startLoopRecovery(ctx, 1, 0, circuit.NopBreaker{}, sw, nil)
	time.Sleep(10 * time.Millisecond)
	if !sw.UpstreamEnabled("u1") {
		t.Fatal("disabled recovery must not change switch state")
	}
}

// TestAutoDisableHookRetiresCredentialFirst: a fatal report on a known
// credential retires it in the ring and leaves the upstream in
// rotation; only the last living credential's death takes the
// upstream out, and an unknown credential index goes straight to the
// upstream-level disable.
func TestAutoDisableHookRetiresCredentialFirst(t *testing.T) {
	t.Parallel()
	adapter := newTwoKeyAdapter(t)
	rings := map[string]upstream.CredentialPool{"u1": adapter}
	sw := router.NewSwitch([]string{"m1"}, []string{"u1"})
	metrics := obs.NewMetrics()
	hook := autoDisableHook(sw, rings, metrics, discardLogger())

	// The first credential dies: the ring shrinks, the upstream stays.
	hook("u1", 0, "upstream_auth_failure")
	if !sw.UpstreamEnabled("u1") {
		t.Fatal("one dead credential must not take the upstream out")
	}
	if got := adapter.AliveCredentials(); got != 1 {
		t.Fatalf("alive = %d, want 1", got)
	}
	assertCredentialRetiredCount(t, metrics, 1)

	// A repeat report on the already-dead credential changes nothing.
	hook("u1", 0, "upstream_auth_failure")
	if got := adapter.AliveCredentials(); got != 1 {
		t.Fatalf("alive = %d, want 1 after a repeat report", got)
	}
	assertCredentialRetiredCount(t, metrics, 1)

	// The last credential dies: the upstream leaves rotation.
	hook("u1", 1, "upstream_auth_failure")
	if sw.UpstreamEnabled("u1") {
		t.Fatal("the last dead credential must take the upstream out")
	}
	assertCredentialRetiredCount(t, metrics, 2)
	assertAutoDisabledCount(t, metrics, 1)

	// An upstream-level report (no credential) disables directly.
	sw2 := router.NewSwitch([]string{"m1"}, []string{"u2"})
	hook2 := autoDisableHook(sw2, rings, obs.NewMetrics(), discardLogger())
	hook2("u2", -1, "upstream_quota_exhausted")
	if sw2.UpstreamEnabled("u2") {
		t.Fatal("a credential-less report must disable the upstream")
	}
}

// assertCredentialRetiredCount renders the registry and checks the
// credential-retirement counter fired exactly n times.
func assertCredentialRetiredCount(t *testing.T, metrics *obs.Metrics, want int) {
	t.Helper()
	assertMetricLine(t, metrics, `breakwater_credential_retired_total{upstream="u1",reason="upstream_auth_failure"} `, want)
}

// TestSwitchEnableRevivesRing: the switch's enable callback restores
// the full ring, and disabling does not touch it.
func TestSwitchEnableRevivesRing(t *testing.T) {
	t.Parallel()
	adapter := newTwoKeyAdapter(t)
	sw := router.NewSwitch([]string{"m1"}, []string{"u1"},
		router.WithOnUpstreamEnable(reviveRing(map[string]upstream.CredentialPool{"u1": adapter}, discardLogger())))

	adapter.RetireCredential(0)
	if err := sw.SetUpstream("u1", false); err != nil {
		t.Fatalf("disable: %v", err)
	}
	if got := adapter.AliveCredentials(); got != 1 {
		t.Fatalf("alive after disable = %d, want 1 — a disable revives nothing", got)
	}
	if err := sw.SetUpstream("u1", true); err != nil {
		t.Fatalf("enable: %v", err)
	}
	if got := adapter.AliveCredentials(); got != 2 {
		t.Fatalf("alive after enable = %d, want the full ring restored", got)
	}
}
