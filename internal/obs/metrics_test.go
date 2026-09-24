/**
 * @file metrics_test
 * @description Registry tests: exposition format shape, histogram
 * buckets, nil-receiver safety.
 */
package obs

import (
	"strings"
	"testing"
)

func TestMetricsRenderCountersAndGauges(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	m.RateLimited("t1")
	m.RateLimited("t1")
	m.RateLimited("t2")
	m.InflightAdd(3)

	var out strings.Builder
	m.Render(&out)
	text := out.String()

	for _, want := range []string{
		"# TYPE breakwater_rate_limited_total counter",
		`breakwater_rate_limited_total{tenant="t1"} 2`,
		`breakwater_rate_limited_total{tenant="t2"} 1`,
		"breakwater_inflight_requests 3",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("exposition missing %q\ngot:\n%s", want, text)
		}
	}
}

func TestMetricsRenderHistogram(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	m.ObserveDuration("u1", 0.004) // bucket .005
	m.ObserveDuration("u1", 0.02)  // bucket .025

	var out strings.Builder
	m.Render(&out)
	text := out.String()

	for _, want := range []string{
		"# TYPE breakwater_request_duration_seconds histogram",
		`breakwater_request_duration_seconds_bucket{upstream="u1",le="0.005"} 1`,
		`breakwater_request_duration_seconds_bucket{upstream="u1",le="0.025"} 2`,
		`breakwater_request_duration_seconds_bucket{upstream="u1",le="+Inf"} 2`,
		`breakwater_request_duration_seconds_count{upstream="u1"} 2`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("exposition missing %q\ngot:\n%s", want, text)
		}
	}
}

func TestMetricsCircuitStateGauge(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	m.CircuitOpened("u1")
	m.CircuitState("u1", 2)

	var out strings.Builder
	m.Render(&out)
	text := out.String()
	if !strings.Contains(text, `breakwater_circuit_state{upstream="u1"} 2`) {
		t.Errorf("circuit gauge missing:\n%s", text)
	}
	if !strings.Contains(text, `breakwater_circuit_open_total{upstream="u1"} 1`) {
		t.Errorf("circuit open counter missing:\n%s", text)
	}
}

// TestMetricsLogsDroppedRendersFromSync pins that the drop counter is
// exposed from the first scrape — including the healthy zero, which
// must still produce a sample line for the scraper — and that the
// periodic sync moves the rendered value.
func TestMetricsLogsDroppedRendersFromSync(t *testing.T) {
	t.Parallel()
	m := NewMetrics()

	var out strings.Builder
	m.Render(&out)
	if !strings.Contains(out.String(), "breakwater_logs_dropped_total 0") {
		t.Errorf("healthy zero missing from exposition:\n%s", out.String())
	}

	m.SetLogsDropped(7)
	out.Reset()
	m.Render(&out)
	if !strings.Contains(out.String(), "breakwater_logs_dropped_total 7") {
		t.Errorf("drop counter missing from exposition:\n%s", out.String())
	}
}

// TestMetricsNilSafety pins that disabled instrumentation is free of
// nil panics at every call site.
func TestMetricsNilSafety(t *testing.T) {
	t.Parallel()
	var m *Metrics
	m.Request("t", "m", "u", 200)
	m.ObserveDuration("u", 0.5)
	m.InflightAdd(1)
	m.RateLimited("t")
	m.QuotaReserved("t", 10)
	m.QuotaRefunded("t", 5)
	m.QuotaExpired()
	m.CacheHit()
	m.CacheMiss()
	m.CacheFetch("u")
	m.CacheShared()
	m.CircuitOpened("u")
	m.CircuitHalfOpen("u")
	m.CircuitState("u", 1)
	m.RetryScheduled("u")
	m.RetryBudgetExhausted()
	m.Failover("a", "b")
	m.StreamAborted("u")
	m.SetLogsDropped(3)
}
