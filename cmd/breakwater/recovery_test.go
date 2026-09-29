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

// assertAutoDisabledCount renders the registry and checks the
// auto-disable counter fired exactly n times.
func assertAutoDisabledCount(t *testing.T, metrics *obs.Metrics, want int) {
	t.Helper()
	var out strings.Builder
	if err := metrics.Render(&out); err != nil {
		t.Fatalf("render: %v", err)
	}
	wantLine := `breakwater_upstream_auto_disabled_total{upstream="u1",reason="upstream_auth_failure"} `
	if !strings.Contains(out.String(), wantLine+strconv.Itoa(want)) {
		t.Fatalf("exposition missing %s%d:\n%s", wantLine, want, out.String())
	}
}

// newProbeTarget spins up an upstream whose probe endpoint answers
// with the given status, together with its adapter.
func newProbeTarget(t *testing.T, status int) (upstream.Upstream, *httptest.Server) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	adapter, err := upstream.NewOpenAI(upstream.OpenAIConfig{
		ID:       "u1",
		BaseURL:  srv.URL,
		ProbeURL: srv.URL + "/healthz",
	})
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	return adapter, srv
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

// TestRecoveryLiftsAutoDisableOnHealthyProbe: an auto-disabled
// upstream is probed and comes back when the probe answers 2xx.
func TestRecoveryLiftsAutoDisableOnHealthyProbe(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	adapter, _ := newProbeTarget(t, http.StatusOK)
	sw := router.NewSwitch([]string{"m1"}, []string{"u1"})
	if _, err := sw.AutoDisableUpstream("u1", "upstream_auth_failure"); err != nil {
		t.Fatalf("auto disable: %v", err)
	}

	startRecovery(ctx, 5*time.Millisecond, time.Second, 0, 1, circuit.NopBreaker{}, sw,
		map[string]upstream.Upstream{"u1": adapter}, obs.NewMetrics(), discardLogger())

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
	adapter, err := upstream.NewOpenAI(upstream.OpenAIConfig{
		ID: "u1", BaseURL: srv.URL, ProbeURL: srv.URL + "/healthz",
	})
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	sw := router.NewSwitch([]string{"m1"}, []string{"u1"})
	if _, err := sw.AutoDisableUpstream("u1", "upstream_auth_failure"); err != nil {
		t.Fatalf("auto disable: %v", err)
	}

	startRecovery(ctx, 5*time.Millisecond, time.Second, 0, 2, circuit.NopBreaker{}, sw,
		map[string]upstream.Upstream{"u1": adapter}, obs.NewMetrics(), discardLogger())

	time.Sleep(80 * time.Millisecond)
	if sw.UpstreamEnabled("u1") {
		t.Fatal("one pass then a failure must leave the upstream out of rotation")
	}
}

// TestRecoveryRestoresAtThreshold: with a permanently healthy probe
// the upstream comes back once the consecutive-pass count is met.
func TestRecoveryRestoresAtThreshold(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	adapter, _ := newProbeTarget(t, http.StatusOK)
	sw := router.NewSwitch([]string{"m1"}, []string{"u1"})
	if _, err := sw.AutoDisableUpstream("u1", "upstream_quota_exhausted"); err != nil {
		t.Fatalf("auto disable: %v", err)
	}

	startRecovery(ctx, 5*time.Millisecond, time.Second, 0, 3, circuit.NopBreaker{}, sw,
		map[string]upstream.Upstream{"u1": adapter}, obs.NewMetrics(), discardLogger())

	waitFor(t, 2*time.Second, func() bool { return sw.UpstreamEnabled("u1") })
}

// TestRecoveryKeepsUnhealthyUpstreamOut: a failing probe keeps the
// auto disable in place.
func TestRecoveryKeepsUnhealthyUpstreamOut(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	adapter, _ := newProbeTarget(t, http.StatusServiceUnavailable)
	sw := router.NewSwitch([]string{"m1"}, []string{"u1"})
	if _, err := sw.AutoDisableUpstream("u1", "upstream_quota_exhausted"); err != nil {
		t.Fatalf("auto disable: %v", err)
	}

	startRecovery(ctx, 5*time.Millisecond, time.Second, 0, 1, circuit.NopBreaker{}, sw,
		map[string]upstream.Upstream{"u1": adapter}, obs.NewMetrics(), discardLogger())

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
	adapter, _ := newProbeTarget(t, http.StatusOK)
	sw := router.NewSwitch([]string{"m1"}, []string{"u1"})
	if err := sw.SetUpstream("u1", false); err != nil {
		t.Fatalf("manual disable: %v", err)
	}

	startRecovery(ctx, 5*time.Millisecond, time.Second, 0, 1, circuit.NopBreaker{}, sw,
		map[string]upstream.Upstream{"u1": adapter}, obs.NewMetrics(), discardLogger())

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
	if _, err := sw.AutoDisableUpstream("u2", "upstream_auth_failure"); err != nil {
		t.Fatalf("auto disable: %v", err)
	}

	// u1 is probe-capable but healthy-idle; u2 has no target.
	adapter, _ := newProbeTarget(t, http.StatusOK)
	startRecovery(ctx, 5*time.Millisecond, time.Second, 0, 1, circuit.NopBreaker{}, sw,
		map[string]upstream.Upstream{"u1": adapter}, obs.NewMetrics(), discardLogger())

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
	adapter, _ := newProbeTarget(t, http.StatusOK)
	breaker := circuit.NewRegistry(circuit.Config{FailThreshold: 1, Cooldown: time.Millisecond})
	perm, ok := breaker.Allow(ctx, "u1")
	if !ok {
		t.Fatal("closed breaker denied the setup call")
	}
	perm.Report(circuit.OutcomeServerFault)
	if breaker.StateOf(ctx, "u1") != circuit.StateOpen {
		t.Fatal("setup: breaker did not open")
	}
	sw := router.NewSwitch([]string{"m1"}, []string{"u1"})

	startRecovery(ctx, 5*time.Millisecond, time.Second, 0, 1, breaker, sw,
		map[string]upstream.Upstream{"u1": adapter}, obs.NewMetrics(), discardLogger())

	waitFor(t, 2*time.Second, func() bool { return breaker.StateOf(ctx, "u1") == circuit.StateClosed })
}

// TestRecoveryWaitsOutBreakerCooldown: while the open breaker's
// cooldown is still running, the recovery probe is denied like any
// other attempt — the loop must not cut the cooldown short.
func TestRecoveryWaitsOutBreakerCooldown(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	adapter, _ := newProbeTarget(t, http.StatusOK)
	breaker := circuit.NewRegistry(circuit.Config{FailThreshold: 1, Cooldown: time.Hour})
	perm, ok := breaker.Allow(ctx, "u1")
	if !ok {
		t.Fatal("closed breaker denied the setup call")
	}
	perm.Report(circuit.OutcomeServerFault)
	sw := router.NewSwitch([]string{"m1"}, []string{"u1"})

	startRecovery(ctx, 5*time.Millisecond, time.Second, 0, 1, breaker, sw,
		map[string]upstream.Upstream{"u1": adapter}, obs.NewMetrics(), discardLogger())

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
	adapter, _ := newProbeTarget(t, http.StatusOK)
	targets := map[string]upstream.Upstream{"u1": adapter}

	// Neither call may panic nor change state; both return immediately.
	startRecovery(ctx, 0, time.Second, 0, 1, circuit.NopBreaker{}, sw, targets, obs.NewMetrics(), discardLogger())
	startRecovery(ctx, 5*time.Millisecond, time.Second, 0, 1, circuit.NopBreaker{}, sw, nil, obs.NewMetrics(), discardLogger())
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
	adapter, err := upstream.NewOpenAI(upstream.OpenAIConfig{
		ID: "u1", BaseURL: "http://127.0.0.1:8090",
		APIKeys: []string{"k1", "k2"},
	})
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
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
	var out strings.Builder
	if err := metrics.Render(&out); err != nil {
		t.Fatalf("render: %v", err)
	}
	wantLine := `breakwater_credential_retired_total{upstream="u1",reason="upstream_auth_failure"} `
	if !strings.Contains(out.String(), wantLine+strconv.Itoa(want)) {
		t.Fatalf("exposition missing %s%d:\n%s", wantLine, want, out.String())
	}
}

// TestSwitchEnableRevivesRing: the switch's enable callback restores
// the full ring, and disabling does not touch it.
func TestSwitchEnableRevivesRing(t *testing.T) {
	t.Parallel()
	adapter, err := upstream.NewOpenAI(upstream.OpenAIConfig{
		ID: "u1", BaseURL: "http://127.0.0.1:8090",
		APIKeys: []string{"k1", "k2"},
	})
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
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
