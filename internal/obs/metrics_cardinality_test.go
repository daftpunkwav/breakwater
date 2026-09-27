/**
 * @file metrics_cardinality_test
 * @description The registry's bound on label sets: what happens to a
 * family that a hostile or buggy client fills past its cap, and what
 * the exposition shows afterwards.
 */
package obs

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

// TestChildCapCollapsesPastTheLimit pins the cardinality floor: past
// maxChildren new label sets share one reserved overflow leaf, so the
// family stays bounded without a counter that silently stops moving.
func TestChildCapCollapsesPastTheLimit(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	for i := range maxChildren + 25 {
		m.Request("t", fmt.Sprintf("model-%d", i), "u", 200)
	}
	f := m.families["breakwater_requests_total"]
	if len(f.children) != maxChildren+1 {
		t.Fatalf("children = %d, want the cap plus the overflow leaf", len(f.children))
	}
	// An existing leaf keeps counting, and the traffic past the cap is
	// still counted — just without per-label detail.
	m.Request("t", "model-0", "u", 200)
	m.Request("t", "model-0", "u", 200)
	if len(f.children) != maxChildren+1 {
		t.Fatalf("children = %d after repeat traffic, want the cap to hold", len(f.children))
	}
	var buf bytes.Buffer
	if err := m.Render(&buf); err != nil {
		t.Fatalf("render: %v", err)
	}
	// The capped families here are labelled by upstream and by tenant,
	// so their overflow leaf is visible under those names.
	if !strings.Contains(buf.String(), fmt.Sprintf("upstream=%q", labelCapOverflow)) {
		t.Fatalf("exposition missing the overflow leaf:\n%s", buf.String())
	}
	ov := f.children[overflowKey]
	if ov == nil {
		t.Fatal("no overflow leaf")
	}
	if got := ov.count.Load(); got != 25 {
		t.Fatalf("overflow count = %d, want the 25 capped requests to be counted", got)
	}
}

// TestCappedRecordersStillCount: past the cap every recorder shape —
// counters, gauges and both histograms — keeps working through the
// overflow leaf instead of panicking or going quiet.
//
// Each family is filled to its own cap first. That is the point of the
// test: a histogram's overflow leaf must get bucket storage like any
// other leaf, because Render walks those buckets and a nil slice there
// would panic the whole exposition.
func TestCappedRecordersStillCount(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	for i := range maxChildren {
		up := fmt.Sprintf("u%d", i)
		m.CacheFetch(up)  // counter
		m.RateLimited(up) // counter
		m.CircuitState(up, 1)
		m.ObserveDuration(up, 1) // histogram
		m.ObserveTTFT(up, 1)     // histogram
	}
	m.CacheFetch("overflow")
	m.RateLimited("overflow")
	m.CircuitState("overflow", 2)
	m.ObserveDuration("overflow", 1)
	m.ObserveTTFT("overflow", 1)

	for _, name := range []string{
		"breakwater_cache_upstream_fetch_total",
		"breakwater_rate_limited_total",
		"breakwater_circuit_state",
		"breakwater_request_duration_seconds",
		"breakwater_upstream_ttft_seconds",
	} {
		f := m.families[name]
		if f == nil {
			t.Fatalf("family %q missing", name)
		}
		if len(f.children) != maxChildren+1 {
			t.Errorf("%s: children = %d, want the cap plus the overflow leaf", name, len(f.children))
		}
		if f.children[overflowKey] == nil {
			t.Errorf("%s: no overflow leaf", name)
		}
		if f.typ == "histogram" && f.children[overflowKey].buckets == nil {
			t.Errorf("%s: overflow histogram leaf has no bucket storage", name)
		}
	}
	var buf bytes.Buffer
	if err := m.Render(&buf); err != nil {
		t.Fatalf("render: %v", err)
	}
	// The capped families here are labelled by upstream and by tenant,
	// so their overflow leaf is visible under those names.
	if !strings.Contains(buf.String(), fmt.Sprintf("upstream=%q", labelCapOverflow)) {
		t.Fatalf("exposition missing the overflow leaf:\n%s", buf.String())
	}
}
