/**
 * @file recoverybackoff_test
 * @description The failed-probe backoff of the recovery loop: the
 * doubling ladder with its ceiling, the per-upstream schedule that
 * skips an upstream until its next attempt is due, the forget on a
 * healthy answer, and the fixed pace the zero default keeps.
 */
package main

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/daftpunkwav/breakwater/internal/circuit"
	"github.com/daftpunkwav/breakwater/internal/obs"
	"github.com/daftpunkwav/breakwater/internal/router"
	"github.com/daftpunkwav/breakwater/internal/upstream"
)

// backoffInterval is the direct-call tests' base interval: long enough
// that "immediately after" is always inside the backoff window.
const backoffInterval = 100 * time.Millisecond

// backoffCeiling is the direct-call tests' ladder ceiling: room for
// two rungs above the base interval.
const backoffCeiling = 400 * time.Millisecond

// TestProbeBackoffLadder: the wait starts at one base interval and
// doubles per consecutive failure, never past the ceiling and never
// overflowing, however long the streak grows.
func TestProbeBackoffLadder(t *testing.T) {
	t.Parallel()
	base, ceiling := time.Second, 8*time.Second
	for _, tc := range []struct {
		fails int
		want  time.Duration
	}{
		{1, 1 * time.Second},
		{2, 2 * time.Second},
		{3, 4 * time.Second},
		{4, 8 * time.Second},
		{5, 8 * time.Second},
		{100, 8 * time.Second},
	} {
		if got := probeBackoff(base, ceiling, tc.fails); got != tc.want {
			t.Errorf("probeBackoff(fails=%d) = %s, want %s", tc.fails, got, tc.want)
		}
	}
	// A ceiling below the base caps every wait, including the first.
	if got := probeBackoff(10*time.Second, time.Second, 1); got != time.Second {
		t.Errorf("probeBackoff below-base ceiling = %s, want the ceiling", got)
	}
}

// probeFailCount renders the registry and reads the upstream probe
// counter's failure value for one upstream.
func probeFailCount(t *testing.T, metrics *obs.Metrics, id string) int {
	t.Helper()
	var out strings.Builder
	if err := metrics.Render(&out); err != nil {
		t.Fatalf("render: %v", err)
	}
	prefix := `breakwater_upstream_probe_total{upstream="` + id + `",result="fail"} `
	for _, line := range strings.Split(out.String(), "\n") {
		if after, ok := strings.CutPrefix(line, prefix); ok {
			n, err := strconv.Atoi(strings.TrimSpace(after))
			if err != nil {
				t.Fatalf("probe counter line %q: %v", line, err)
			}
			return n
		}
	}
	return 0
}

// runRecoveryPass drives one tick of the auto-disabled recovery body
// with a fresh deadline.
func runRecoveryPass(timeout, backoff time.Duration, sw *router.Switch, probes map[string]upstream.Upstream, book probeBook, metrics *obs.Metrics) {
	recoverAutoDisabled(context.Background(), backoffInterval, timeout, backoff, 1,
		sw, probes, book, metrics, discardLogger())
}

// TestRecoveryBackoffSkipsUndueUpstreams drives the loop body directly:
// a failed probe schedules the next attempt one ladder rung out, ticks
// inside that window skip the upstream, and expiry re-probes onto the
// next rung.
func TestRecoveryBackoffSkipsUndueUpstreams(t *testing.T) {
	t.Parallel()
	adapter, _ := newProbeTarget(t, http.StatusServiceUnavailable)
	sw := router.NewSwitch([]string{"m1"}, []string{"u1"})
	if _, err := sw.AutoDisableUpstream("u1", "upstream_auth_failure"); err != nil {
		t.Fatalf("auto disable: %v", err)
	}
	metrics := obs.NewMetrics()
	book := probeBook{passes: make(map[string]int), retries: make(map[string]probeRetry)}
	probes := map[string]upstream.Upstream{"u1": adapter}

	// First failure: probed, and the next attempt lands one interval
	// out.
	runRecoveryPass(time.Second, backoffCeiling, sw, probes, book, metrics)
	if got := probeFailCount(t, metrics, "u1"); got != 1 {
		t.Fatalf("probe count = %d, want 1", got)
	}
	r := book.retries["u1"]
	if r.fails != 1 || !r.next.After(time.Now()) {
		t.Fatalf("schedule after first failure = %+v, want fails 1 and a future next", r)
	}

	// A pass inside the backoff window must skip the upstream: no new
	// probe, schedule untouched.
	runRecoveryPass(time.Second, backoffCeiling, sw, probes, book, metrics)
	if got := probeFailCount(t, metrics, "u1"); got != 1 {
		t.Fatalf("probe count after undue pass = %d, want 1", got)
	}
	if book.retries["u1"] != r {
		t.Fatalf("schedule changed on a skipped pass: %+v", book.retries["u1"])
	}

	// Once due, the probe fires onto the next rung: double the wait.
	book.retries["u1"] = probeRetry{fails: r.fails, next: time.Now().Add(-time.Millisecond)}
	runRecoveryPass(time.Second, backoffCeiling, sw, probes, book, metrics)
	if got := probeFailCount(t, metrics, "u1"); got != 2 {
		t.Fatalf("probe count after due pass = %d, want 2", got)
	}
	r2 := book.retries["u1"]
	if r2.fails != 2 {
		t.Fatalf("fails = %d, want 2", r2.fails)
	}
	if wait := time.Until(r2.next); wait < backoffInterval || wait > 2*backoffInterval+50*time.Millisecond {
		t.Fatalf("second wait = %s, want one doubled interval", wait)
	}
}

// TestRecoveryBackoffZeroKeepsFixedPace: with the backoff disabled a
// due check never skips — every pass probes, whatever the failures.
func TestRecoveryBackoffZeroKeepsFixedPace(t *testing.T) {
	t.Parallel()
	adapter, _ := newProbeTarget(t, http.StatusServiceUnavailable)
	sw := router.NewSwitch([]string{"m1"}, []string{"u1"})
	if _, err := sw.AutoDisableUpstream("u1", "upstream_auth_failure"); err != nil {
		t.Fatalf("auto disable: %v", err)
	}
	metrics := obs.NewMetrics()
	book := probeBook{passes: make(map[string]int), retries: make(map[string]probeRetry)}
	probes := map[string]upstream.Upstream{"u1": adapter}

	for i := 0; i < 3; i++ {
		runRecoveryPass(time.Second, 0, sw, probes, book, metrics)
	}
	if got := probeFailCount(t, metrics, "u1"); got != 3 {
		t.Fatalf("probe count = %d, want 3 (no backoff, no skips)", got)
	}
}

// TestRecoveryBackoffStillRestores: the ladder must not endanger
// recovery — a healthy endpoint lifts the auto disable with the
// backoff armed, through the real loop.
func TestRecoveryBackoffStillRestores(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	adapter, _ := newProbeTarget(t, http.StatusOK)
	sw := router.NewSwitch([]string{"m1"}, []string{"u1"})
	if _, err := sw.AutoDisableUpstream("u1", "upstream_auth_failure"); err != nil {
		t.Fatalf("auto disable: %v", err)
	}

	startRecovery(ctx, 5*time.Millisecond, time.Second, time.Second, 1, circuit.NopBreaker{}, sw,
		map[string]upstream.Upstream{"u1": adapter}, obs.NewMetrics(), discardLogger())

	waitFor(t, 2*time.Second, func() bool { return sw.UpstreamEnabled("u1") })
}

// TestRecoveryBackoffPacesTheRealLoop: an always-failing upstream with
// the ladder armed is probed a handful of times, not once per tick.
func TestRecoveryBackoffPacesTheRealLoop(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	adapter, _ := newProbeTarget(t, http.StatusServiceUnavailable)
	sw := router.NewSwitch([]string{"m1"}, []string{"u1"})
	if _, err := sw.AutoDisableUpstream("u1", "upstream_auth_failure"); err != nil {
		t.Fatalf("auto disable: %v", err)
	}
	metrics := obs.NewMetrics()

	startRecovery(ctx, 10*time.Millisecond, time.Second, 80*time.Millisecond, 1, circuit.NopBreaker{}, sw,
		map[string]upstream.Upstream{"u1": adapter}, metrics, discardLogger())

	time.Sleep(150 * time.Millisecond)
	// The ladder allows probes at ~0, 10, 30, 70, 150ms; a missing
	// backoff would probe every 10ms tick (~15 times).
	if got := probeFailCount(t, metrics, "u1"); got > 8 {
		t.Fatalf("probe count = %d, want the backoff ladder (<= 8)", got)
	}
}
