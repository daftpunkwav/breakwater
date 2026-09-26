/**
 * @file insights_endpoint_test
 * @description The assessment endpoint: the window parameter bounds,
 * the outage mapping, and the 404 without a store.
 */
package server

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/daftpunkwav/breakwater/internal/insights"
)

// scriptedReporter answers Report with a fixed result and records the
// window it was asked for.
type scriptedReporter struct {
	report insights.Report
	err    error
	from   time.Time
	to     time.Time
}

func (s *scriptedReporter) Report(_ context.Context, from, to time.Time) (insights.Report, error) {
	s.from, s.to = from, to
	return s.report, s.err
}

func TestInsightsEndpointRendersReport(t *testing.T) {
	t.Parallel()
	reporter := &scriptedReporter{report: insights.Report{
		Summary: insights.Summary{Requests: 10, Failures: 2, SuccessRate: 0.8},
	}}
	admin := NewAdmin("", nil, nil, nil, WithInsights(reporter))

	req := httptest.NewRequest(http.MethodGet, "/admin/insights?hours=48", nil)
	rec := httptest.NewRecorder()
	admin.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"requests":10`) {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	// The 48-hour request bounds the window 48 hours wide.
	if got := reporter.to.Sub(reporter.from); got != 48*time.Hour {
		t.Fatalf("window = %s, want 48h", got)
	}
}

func TestInsightsEndpointRejectsBadWindows(t *testing.T) {
	t.Parallel()
	admin := NewAdmin("", nil, nil, nil, WithInsights(&scriptedReporter{}))
	for _, raw := range []string{"?hours=0", "?hours=721", "?hours=abc", "?hours=-5"} {
		req := httptest.NewRequest(http.MethodGet, "/admin/insights"+raw, nil)
		rec := httptest.NewRecorder()
		admin.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("%s status = %d, want 400", raw, rec.Code)
		}
	}
}

// TestInsightsEndpointOutageAndClosure: a store failure is a 503, and
// without an installed store the endpoint closes.
func TestInsightsEndpointOutageAndClosure(t *testing.T) {
	t.Parallel()
	admin := NewAdmin("", nil, nil, nil, WithInsights(&scriptedReporter{err: errors.New("connection refused")}))
	req := httptest.NewRequest(http.MethodGet, "/admin/insights", nil)
	rec := httptest.NewRecorder()
	admin.ServeHTTP(rec, req)
	if rec.Code != http.StatusServiceUnavailable || !strings.Contains(rec.Body.String(), "insights_unavailable") {
		t.Fatalf("outage status = %d body = %s, want 503 envelope", rec.Code, rec.Body.String())
	}

	bare := NewAdmin("", nil, nil, nil)
	req = httptest.NewRequest(http.MethodGet, "/admin/insights", nil)
	rec = httptest.NewRecorder()
	bare.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("bare status = %d, want 404", rec.Code)
	}
}
