/**
 * @file observation_stage_test
 * @description The observation stage: the metrics and access-log
 * dimensions of a finished request, the aborted and cache-hit
 * dimensions, and tolerance for absent recorders and carriers.
 */
package pipeline

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/daftpunkwav/breakwater/internal/auth"
	"github.com/daftpunkwav/breakwater/internal/obs"
	"github.com/daftpunkwav/breakwater/internal/protocol"
	"github.com/daftpunkwav/breakwater/internal/relay"
)

// renderMetrics renders the registry to a string for substring
// assertions on the Prometheus text format.
func renderMetrics(t *testing.T, m *obs.Metrics) string {
	t.Helper()
	var sb strings.Builder
	if err := m.Render(&sb); err != nil {
		t.Fatalf("render metrics: %v", err)
	}
	return sb.String()
}

// serveThroughObservation runs one request through the observation
// stage with a terminal handler that writes the given status.
func serveThroughObservation(t *testing.T, m *obs.Metrics, sink obs.Sink, carrier *Carrier, status int) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	if carrier != nil {
		req = req.WithContext(WithCarrier(context.Background(), carrier))
	}

	handler := ObservationStage(m, sink)(http.HandlerFunc(
		func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(status)
			_, _ = w.Write([]byte("payload"))
		}))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

// TestObservationStageRecordsRequestDimensions: the finished request
// lands in the request counter, the duration histogram and the access
// log with tenant/model/upstream/status attached.
func TestObservationStageRecordsRequestDimensions(t *testing.T) {
	t.Parallel()

	metrics := obs.NewMetrics()
	sink := &recordingSink{}
	carrier := &Carrier{
		Tenant:   auth.Tenant{ID: "tenant-1"},
		Chat:     protocol.ChatRequest{Model: "model-1"},
		CacheHit: true,
		Relay:    &relay.Result{UpstreamID: "up-1"},
	}

	rec := serveThroughObservation(t, metrics, sink, carrier, http.StatusCreated)

	rendered := renderMetrics(t, metrics)
	if !strings.Contains(rendered,
		`breakwater_requests_total{tenant="tenant-1",model="model-1",upstream="up-1",status="201"} 1`) {
		t.Fatalf("request counter missing: %s", rendered)
	}
	if !strings.Contains(rendered, `breakwater_request_duration_seconds_count{upstream="up-1"} 1`) {
		t.Fatalf("duration histogram missing: %s", rendered)
	}
	if !strings.Contains(rendered, "breakwater_inflight_requests 0") {
		t.Fatalf("in-flight gauge did not return to zero: %s", rendered)
	}

	entries := sink.snapshot()
	if len(entries) != 1 {
		t.Fatalf("sink recorded %d entries, want 1", len(entries))
	}
	entry := entries[0]
	if entry.TenantID != "tenant-1" || entry.Model != "model-1" || entry.Upstream != "up-1" {
		t.Fatalf("entry dimensions = %+v, want tenant/model/upstream recorded", entry)
	}
	if entry.Method != http.MethodPost || entry.Path != "/v1/chat/completions" || entry.Status != http.StatusCreated {
		t.Fatalf("entry request facts = %+v, want POST /v1/chat/completions 201", entry)
	}
	if !entry.CacheHit {
		t.Fatal("entry.CacheHit = false, want the carrier dimension")
	}
	if entry.Time.IsZero() || entry.Duration < 0 {
		t.Fatalf("entry timing = %+v, want a start time and a non-negative duration", entry)
	}
	if rec.Code != http.StatusCreated || rec.Body.String() != "payload" {
		t.Fatalf("response altered by the stage: %d %q", rec.Code, rec.Body.String())
	}
}

// TestObservationStageRecordsStreamAbort: an aborted relay surfaces on
// the stream-abort counter.
func TestObservationStageRecordsStreamAbort(t *testing.T) {
	t.Parallel()

	metrics := obs.NewMetrics()
	carrier := &Carrier{Relay: &relay.Result{UpstreamID: "up-1", Aborted: true}}

	serveThroughObservation(t, metrics, nil, carrier, http.StatusOK)

	rendered := renderMetrics(t, metrics)
	if !strings.Contains(rendered, `breakwater_sse_stream_aborted_total{upstream="up-1"} 1`) {
		t.Fatalf("abort counter missing: %s", rendered)
	}
}

// TestObservationStageWithoutRelay: a carrier with no relay outcome
// observes the placeholder upstream.
func TestObservationStageWithoutRelay(t *testing.T) {
	t.Parallel()

	metrics := obs.NewMetrics()
	sink := &recordingSink{}

	serveThroughObservation(t, metrics, sink, &Carrier{}, http.StatusOK)

	if !strings.Contains(renderMetrics(t, metrics), `upstream="-"`) {
		t.Fatalf("placeholder upstream missing: %s", renderMetrics(t, metrics))
	}
	entries := sink.snapshot()
	if len(entries) != 1 || entries[0].Upstream != "-" || entries[0].CacheHit {
		t.Fatalf("entry = %+v, want placeholder upstream and no cache hit", entries)
	}
}

// TestObservationStageWithoutCarrier: rejected-before-carrier requests
// are still observed with empty dimensions.
func TestObservationStageWithoutCarrier(t *testing.T) {
	t.Parallel()

	metrics := obs.NewMetrics()
	sink := &recordingSink{}

	serveThroughObservation(t, metrics, sink, nil, http.StatusUnauthorized)

	if !strings.Contains(renderMetrics(t, metrics),
		`breakwater_requests_total{tenant="",model="",upstream="-",status="401"} 1`) {
		t.Fatalf("empty-dimension request missing: %s", renderMetrics(t, metrics))
	}
	entries := sink.snapshot()
	if len(entries) != 1 || entries[0].TenantID != "" || entries[0].Model != "" {
		t.Fatalf("entry = %+v, want empty identity dimensions", entries)
	}
}

// TestObservationStageNilRecorders: both recorders may be disabled;
// the request still flows through untouched.
func TestObservationStageNilRecorders(t *testing.T) {
	t.Parallel()

	rec := serveThroughObservation(t, nil, nil, nil, http.StatusOK)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 with recorders disabled", rec.Code)
	}
}
