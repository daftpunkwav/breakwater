/**
 * @file failover_test
 * @description Failover across candidate upstreams: priority-ordered
 * walking, the attempt/retry accounting, and the retry + failover
 * metrics the observation hook records.
 */
package relay

import (
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/daftpunkwav/breakwater/internal/obs"
	"github.com/daftpunkwav/breakwater/internal/upstream"
)

func TestFailoverWalksCandidates(t *testing.T) {
	t.Parallel()
	primary := &stubUpstream{id: "primary", fn: func(context.Context, upstream.Request) (*upstream.Response, error) {
		return jsonResponse(t, http.StatusServiceUnavailable, `{}`), nil
	}}
	fallback := &stubUpstream{id: "fallback", fn: func(context.Context, upstream.Request) (*upstream.Response, error) {
		return jsonResponse(t, http.StatusOK, `{"usage":{"total_tokens":5}}`), nil
	}}
	exec := New(testPolicy(), nil)

	result := execute(t, exec, []upstream.Upstream{primary, fallback}, false, "{}")
	if result.Status != http.StatusOK || result.UpstreamID != "fallback" {
		t.Fatalf("status = %d served by %q, want 200/fallback", result.Status, result.UpstreamID)
	}
	if result.Attempts != 2 || result.Retries != 1 {
		t.Fatalf("attempts = %d retries = %d, want 2/1", result.Attempts, result.Retries)
	}
}

// TestFailoverObservesRetryAndFailoverMetrics pins the observation hook:
// the retry counts against the candidate it targets, and a candidate
// switch records the failover pair.
func TestFailoverObservesRetryAndFailoverMetrics(t *testing.T) {
	t.Parallel()
	primary := &stubUpstream{id: "primary", fn: func(context.Context, upstream.Request) (*upstream.Response, error) {
		return jsonResponse(t, http.StatusInternalServerError, `{}`), nil
	}}
	fallback := &stubUpstream{id: "fallback", fn: func(context.Context, upstream.Request) (*upstream.Response, error) {
		return jsonResponse(t, http.StatusOK, `{"usage":{"total_tokens":5}}`), nil
	}}
	metrics := obs.NewMetrics()
	exec := New(testPolicy(), nil, WithMetrics(metrics))

	result := execute(t, exec, []upstream.Upstream{primary, fallback}, false, "{}")
	if result.Status != http.StatusOK {
		t.Fatalf("status = %d, want 200", result.Status)
	}

	var out strings.Builder
	if err := metrics.Render(&out); err != nil {
		t.Fatalf("render: %v", err)
	}
	text := out.String()
	if !strings.Contains(text, `breakwater_retry_attempts_total{upstream="fallback"} 1`) {
		t.Errorf("retry metric missing:\n%s", text)
	}
	if !strings.Contains(text, `breakwater_upstream_failover_total{from="primary",to="fallback"} 1`) {
		t.Errorf("failover metric missing:\n%s", text)
	}
}

// TestFailoverRehitReportsNoFailover pins that a retry against the same
// candidate (single-fallback list exhausted) is a retry, not a failover.
func TestFailoverRehitReportsNoFailover(t *testing.T) {
	t.Parallel()
	metrics := obs.NewMetrics()
	cand := &stubUpstream{id: "only", fn: func(context.Context, upstream.Request) (*upstream.Response, error) {
		return jsonResponse(t, http.StatusInternalServerError, `{}`), nil
	}}
	exec := New(testPolicy(), nil, WithMetrics(metrics))

	result := execute(t, exec, []upstream.Upstream{cand}, false, "{}")
	if result.Status != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 passthrough", result.Status)
	}

	var out strings.Builder
	if err := metrics.Render(&out); err != nil {
		t.Fatalf("render: %v", err)
	}
	if strings.Contains(out.String(), "breakwater_upstream_failover_total{") {
		t.Errorf("same-candidate retry recorded as failover:\n%s", out.String())
	}
	if !strings.Contains(out.String(), `breakwater_retry_attempts_total{upstream="only"} 2`) {
		t.Errorf("retry attempts not fully recorded:\n%s", out.String())
	}
}
