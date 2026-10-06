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

	assertExpositionContains(t, m,
		"# TYPE breakwater_rate_limited_total counter",
		`breakwater_rate_limited_total{tenant="t1"} 2`,
		`breakwater_rate_limited_total{tenant="t2"} 1`,
		"breakwater_inflight_requests 3",
	)
}

func TestMetricsRenderHistogram(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	m.ObserveDuration("u1", 0.004) // bucket .005
	m.ObserveDuration("u1", 0.02)  // bucket .025

	assertExpositionContains(t, m,
		"# TYPE breakwater_request_duration_seconds histogram",
		`breakwater_request_duration_seconds_bucket{upstream="u1",le="0.005"} 1`,
		`breakwater_request_duration_seconds_bucket{upstream="u1",le="0.025"} 2`,
		`breakwater_request_duration_seconds_bucket{upstream="u1",le="+Inf"} 2`,
		`breakwater_request_duration_seconds_count{upstream="u1"} 2`,
	)
}

func TestMetricsCircuitStateGauge(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	m.CircuitOpened("u1")
	m.CircuitState("u1", 2)

	text := renderExposition(t, m)
	if !strings.Contains(text, `breakwater_circuit_state{upstream="u1"} 2`) {
		t.Errorf("circuit gauge missing:\n%s", text)
	}
	if !strings.Contains(text, `breakwater_circuit_open_total{upstream="u1"} 1`) {
		t.Errorf("circuit open counter missing:\n%s", text)
	}
}

// TestMetricsCircuitDeniedRenders: the ratio-strategy denial counter
// renders per upstream from its first increment.
func TestMetricsCircuitDeniedRenders(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	m.CircuitDenied("u1")
	m.CircuitDenied("u1")

	if text := renderExposition(t, m); !strings.Contains(text, `breakwater_circuit_denied_total{upstream="u1"} 2`) {
		t.Errorf("circuit denied counter missing:\n%s", text)
	}
}

// TestMetricsLogsDroppedRendersFromSync pins that the drop counter is
// exposed from the first scrape — including the healthy zero, which
// must still produce a sample line for the scraper — and that the
// periodic sync moves the rendered value.
func TestMetricsLogsDroppedRendersFromSync(t *testing.T) {
	t.Parallel()
	m := NewMetrics()

	text := renderExposition(t, m)
	if !strings.Contains(text, "breakwater_logs_dropped_total 0") {
		t.Errorf("healthy zero missing from exposition:\n%s", text)
	}

	m.SetLogsDropped(7)
	text = renderExposition(t, m)
	if !strings.Contains(text, "breakwater_logs_dropped_total 7") {
		t.Errorf("drop counter missing from exposition:\n%s", text)
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
	m.ConcurrencyLimited("t")
	m.QuotaReserved("t", 10)
	m.QuotaRefunded("t", 5)
	m.QuotaReconciliationError()
	m.QuotaExpired()
	m.CacheHit()
	m.CacheMiss()
	m.CacheFetch("u")
	m.CacheShared()
	m.CircuitOpened("u")
	m.CircuitHalfOpen("u")
	m.CircuitDenied("u")
	m.CircuitState("u", 1)
	m.RetryScheduled("u")
	m.RetryBudgetExhausted()
	m.Failover("a", "b")
	m.StreamAborted("u")
	m.UpstreamProbe("u", true)
	m.UpstreamAutoDisabled("u", "reason")
	m.CredentialRetired("u", "reason")
	m.ObserveTTFT("u", 0.25)
	m.SetLogsDropped(3)
	m.SetInsightsDropped(2)
}

// TestMetricsRecoveryAndRotationRenders: the probe, auto-disable,
// credential-retirement and concurrency counters and the TTFT
// histogram all render from their first increments.
func TestMetricsRecoveryAndRotationRenders(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	m.UpstreamProbe("u", true)
	m.UpstreamProbe("u", false)
	m.UpstreamAutoDisabled("u", "upstream_auth_failure")
	m.CredentialRetired("u", "upstream_auth_failure")
	m.ConcurrencyLimited("tenant")
	m.ObserveTTFT("u", 0.25)

	assertExpositionContains(t, m,
		`breakwater_upstream_probe_total{upstream="u",result="ok"} 1`,
		`breakwater_upstream_probe_total{upstream="u",result="fail"} 1`,
		`breakwater_upstream_auto_disabled_total{upstream="u",reason="upstream_auth_failure"} 1`,
		`breakwater_credential_retired_total{upstream="u",reason="upstream_auth_failure"} 1`,
		`breakwater_concurrency_limited_total{tenant="tenant"} 1`,
		`breakwater_upstream_ttft_seconds_count{upstream="u"} 1`,
	)
}
