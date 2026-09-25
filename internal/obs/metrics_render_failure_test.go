/**
 * @file metrics_render_failure_test
 * @description Render's error propagation: the first failing write to
 * the scrape sink is returned as-is, whatever exposition line it hit.
 */
package obs

import (
	"errors"
	"strings"
	"testing"
)

// quotaWriter accepts a fixed number of writes, then fails — the
// failure-point sweep drives Render through every return path.
type quotaWriter struct {
	remaining int
}

func (w *quotaWriter) Write(p []byte) (int, error) {
	if w.remaining <= 0 {
		return 0, errors.New("scrape sink broken")
	}
	w.remaining--
	return len(p), nil
}

// TestMetricsRenderPropagatesWriteFailure pins that Render reports the
// sink's write error at every exposition position: for each quota below
// the full exposition, Render must fail instead of reporting success.
func TestMetricsRenderPropagatesWriteFailure(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	recordEverything(m)
	// A histogram child is required so the sweep also reaches the
	// bucket, +Inf, sum and count write paths.
	m.ObserveDuration("u1", 0.004)

	// Count the writes of a healthy full exposition.
	total := (&quotaWriter{remaining: 1 << 16})
	if err := m.Render(total); err != nil {
		t.Fatalf("healthy render failed: %v", err)
	}
	writes := 1<<16 - total.remaining

	for q := 0; q < writes; q++ {
		if err := m.Render(&quotaWriter{remaining: q}); err == nil {
			t.Fatalf("render with write quota %d succeeded, want the sink error", q)
		}
	}
	// The exact quota always succeeds.
	if err := m.Render(&quotaWriter{remaining: writes}); err != nil {
		t.Fatalf("full render failed: %v", err)
	}
}

// TestMetricsRenderErrorCarriesSinkMessage pins that the surfaced error
// is the sink's own failure, not a swallowed generic.
func TestMetricsRenderErrorCarriesSinkMessage(t *testing.T) {
	t.Parallel()
	m := NewMetrics()
	m.RateLimited("t1")

	err := m.Render(&quotaWriter{remaining: 0})
	if err == nil || !strings.Contains(err.Error(), "scrape sink broken") {
		t.Fatalf("err = %v, want the sink failure verbatim", err)
	}
}
