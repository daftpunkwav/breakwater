/**
 * @file observation_failure_taxonomy_test
 * @description The failure taxonomy propagation: the governance
 * rejection code and the key identity reach the access entry, and a
 * forward-stage gateway error refines the classification.
 */
package pipeline

import (
	"net/http"
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
		RejectCode: "quota_insufficient",
		Relay:      &relay.Result{UpstreamID: "u", Status: 503, ErrorCode: "circuit_open"},
	}, http.StatusPaymentRequired)
	if got := sink.snapshot()[0].ErrorCode; got != "quota_insufficient" {
		t.Fatalf("code = %q, want the rejection's quota_insufficient", got)
	}
}
