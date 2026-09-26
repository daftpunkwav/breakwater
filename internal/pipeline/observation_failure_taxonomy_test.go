/**
 * @file observation_failure_taxonomy_test
 * @description The failure taxonomy propagation: the governance
 * rejection code and the key identity reach the access entry, and a
 * forward-stage gateway error refines the classification.
 */
package pipeline

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/daftpunkwav/breakwater/internal/auth"
	"github.com/daftpunkwav/breakwater/internal/obs"
	"github.com/daftpunkwav/breakwater/internal/relay"
)

// TestObservationRecordsRejectionTaxonomy: a request a governance
// stage refused carries its rejection code and the resolving key into
// the entry.
func TestObservationRecordsRejectionTaxonomy(t *testing.T) {
	t.Parallel()
	metrics := obs.NewMetrics()
	sink := &recordingSink{}
	carrier := &Carrier{
		Tenant:     auth.Tenant{ID: "tenant-1", KeyID: "key-9"},
		RejectCode: "rate_limited",
	}

	serveThroughObservation(t, metrics, sink, carrier, http.StatusTooManyRequests)

	entries := sink.snapshot()
	if len(entries) != 1 {
		t.Fatalf("sink recorded %d entries, want 1", len(entries))
	}
	e := entries[0]
	if e.ErrorCode != "rate_limited" {
		t.Fatalf("error code = %q, want rate_limited", e.ErrorCode)
	}
	if e.KeyID != "key-9" {
		t.Fatalf("key id = %q, want key-9", e.KeyID)
	}
}

// TestObservationRefinesWithRelayErrorCode: a gateway-rendered forward
// failure (circuit open, budget exhausted, stream abort) overrides the
// empty rejection code; an upstream passthrough leaves the code empty
// so its status classifies it.
func TestObservationRefinesWithRelayErrorCode(t *testing.T) {
	t.Parallel()
	metrics := obs.NewMetrics()

	sink := &recordingSink{}
	serveThroughObservation(t, metrics, sink, &Carrier{
		Tenant: auth.Tenant{ID: "t"},
		Relay:  &relay.Result{UpstreamID: "u", Status: 503, ErrorCode: "circuit_open"},
	}, http.StatusServiceUnavailable)
	if got := sink.snapshot()[0].ErrorCode; got != "circuit_open" {
		t.Fatalf("gateway code = %q, want circuit_open", got)
	}

	sink = &recordingSink{}
	serveThroughObservation(t, metrics, sink, &Carrier{
		Tenant: auth.Tenant{ID: "t"},
		Relay:  &relay.Result{UpstreamID: "u", Status: 502},
	}, http.StatusBadGateway)
	if got := sink.snapshot()[0].ErrorCode; got != "" {
		t.Fatalf("passthrough code = %q, want empty (status classifies)", got)
	}

	// A rejection code wins over the relay's: the earlier verdict is
	// the cause, the forward never happened.
	sink = &recordingSink{}
	serveThroughObservation(t, metrics, sink, &Carrier{
		Tenant:     auth.Tenant{ID: "t"},
		RejectCode: "insufficient_quota",
		Relay:      &relay.Result{UpstreamID: "u", Status: 503, ErrorCode: "circuit_open"},
	}, http.StatusPaymentRequired)
	if got := sink.snapshot()[0].ErrorCode; got != "insufficient_quota" {
		t.Fatalf("code = %q, want the rejection's insufficient_quota", got)
	}
}

// TestObservationMapsMissingStatusToDisconnect: a response that never
// started (the client walked away before the first byte) is observed
// as status 499 — the disconnect the aggregation never counts as a
// failure — instead of the metric-invisible status 0.
func TestObservationMapsMissingStatusToDisconnect(t *testing.T) {
	t.Parallel()
	metrics := obs.NewMetrics()
	sink := &recordingSink{}

	handler := ObservationStage(metrics, sink)(http.HandlerFunc(
		func(_ http.ResponseWriter, _ *http.Request) {}))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	handler.ServeHTTP(rec, req.WithContext(WithCarrier(context.Background(), &Carrier{Tenant: auth.Tenant{ID: "t"}})))

	e := sink.snapshot()[0]
	if e.Status != obs.StatusClientClosedRequest {
		t.Fatalf("status = %d, want %d", e.Status, obs.StatusClientClosedRequest)
	}
}

// TestObservationCarriesSettlementAndStream: the entry records the
// settled token usage and whether a stream started — the dimensions
// the request_log columns settle and stream.
func TestObservationCarriesSettlementAndStream(t *testing.T) {
	t.Parallel()
	metrics := obs.NewMetrics()
	sink := &recordingSink{}

	serveThroughObservation(t, metrics, sink, &Carrier{
		Tenant:   auth.Tenant{ID: "t"},
		Consumed: 137,
		Relay:    &relay.Result{UpstreamID: "u", Status: 200, Streamed: true},
	}, http.StatusOK)

	e := sink.snapshot()[0]
	if e.Tokens != 137 {
		t.Fatalf("tokens = %d, want 137", e.Tokens)
	}
	if !e.Streamed {
		t.Fatal("streamed = false, want the relay's streaming report")
	}
}
