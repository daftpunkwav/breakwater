/**
 * @file metrics_recorders_test
 * @description The typed recorder methods: every event lands in the
 * exposition with the right labels, the histogram's boundary behavior,
 * and the in-flight gauge's signed adjustments.
 */
package obs

import (
	"math"
	"strings"
	"sync"
	"testing"
)

// recordEverything drives every recorder once so the exposition test
// can assert each family.
func recordEverything(m *Metrics) {
	m.Request("t1", "m1", "u1", 200)
	m.RateLimited("t1")
	m.QuotaReserved("t1", 100)
	m.QuotaRefunded("t1", 40)
	m.QuotaReconciliationError()
	m.QuotaExpired()
	m.CacheHit()
	m.CacheMiss()
	m.CacheFetch("u1")
	m.CacheShared()
	m.CircuitOpened("u1")
	m.CircuitHalfOpen("u1")
	m.CircuitState("u1", 2)
	m.RetryScheduled("u1")
	m.RetryBudgetExhausted()
	m.Failover("a", "b")
	m.StreamAborted("u1")
	m.SetLogsDropped(4)
}

func TestMetricsRecordersEmitSamples(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	recordEverything(m)

	var out strings.Builder
	if err := m.Render(&out); err != nil {
		t.Fatalf("render: %v", err)
	}
	text := out.String()
	for _, want := range []string{
		`breakwater_requests_total{tenant="t1",model="m1",upstream="u1",status="200"} 1`,
		`breakwater_quota_reservation_tokens_total{tenant="t1"} 100`,
		`breakwater_quota_refunded_tokens_total{tenant="t1"} 40`,
		"breakwater_quota_reconciliation_error 1",
		"breakwater_quota_reservation_expired_total 1",
		"breakwater_cache_hit_total 1",
		"breakwater_cache_miss_total 1",
		`breakwater_cache_upstream_fetch_total{upstream="u1"} 1`,
		"breakwater_cache_shared_fetch_total 1",
		`breakwater_circuit_half_open_total{upstream="u1"} 1`,
		`breakwater_retry_attempts_total{upstream="u1"} 1`,
		"breakwater_retry_budget_exhausted_total 1",
		`breakwater_upstream_failover_total{from="a",to="b"} 1`,
		`breakwater_sse_stream_aborted_total{upstream="u1"} 1`,
		"breakwater_logs_dropped_total 4",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("exposition missing %q\ngot:\n%s", want, text)
		}
	}
}

// TestMetricsObserveDurationBeyondLargestBucket pins the histogram
// boundary: an observation past the last bound counts only in the +Inf
// bucket (and sum/count), never in a finite bucket.
func TestMetricsObserveDurationBeyondLargestBucket(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	m.ObserveDuration("u1", 120) // past the 60s top bucket

	var out strings.Builder
	if err := m.Render(&out); err != nil {
		t.Fatalf("render: %v", err)
	}
	text := out.String()
	for _, want := range []string{
		`breakwater_request_duration_seconds_bucket{upstream="u1",le="60"} 0`,
		`breakwater_request_duration_seconds_bucket{upstream="u1",le="+Inf"} 1`,
		`breakwater_request_duration_seconds_count{upstream="u1"} 1`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("exposition missing %q\ngot:\n%s", want, text)
		}
	}
}

// TestMetricsInflightAcceptsNegativeAdjustment pins the gauge contract:
// completion decrements are signed, so a negative adjustment renders as
// a negative value.
func TestMetricsInflightAcceptsNegativeAdjustment(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	m.InflightAdd(3)
	m.InflightAdd(-5)

	var out strings.Builder
	if err := m.Render(&out); err != nil {
		t.Fatalf("render: %v", err)
	}
	if !strings.Contains(out.String(), "breakwater_inflight_requests -2") {
		t.Errorf("in-flight gauge wrong:\n%s", out.String())
	}
}

// TestMetricsChildOfConcurrentCreation hammers one family from many
// goroutines: leaf creation must be race-free and never double-allocate
// for the same label set (run under -race).
func TestMetricsChildOfConcurrentCreation(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	f := m.families["breakwater_rate_limited_total"]

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				f.childOf("tenant", "shared")
			}
		}()
	}
	wg.Wait()
	if len(f.children) != 1 {
		t.Fatalf("children = %d, want exactly one leaf per label set", len(f.children))
	}
}

// TestLabelsWithLEPins the bucket-label rendering, including the bare
// label set of unlabeled histograms.
func TestLabelsWithLE(t *testing.T) {
	t.Parallel()
	if got := labelsWithLE("", 1.5); got != `{le="1.5"}` {
		t.Fatalf("empty labels = %q, want the bare le set", got)
	}
	if got := labelsWithLE(`{upstream="u1"}`, math.Inf(1)); got != `{upstream="u1",le="+Inf"}` {
		t.Fatalf("labeled form = %q", got)
	}
}
