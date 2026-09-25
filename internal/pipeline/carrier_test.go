/**
 * @file carrier_test
 * @description The per-request carrier contract: the once-only body
 * write, the context round-trip, the required-carrier guard and the
 * chain-entry stage that assembles it.
 */
package pipeline

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/daftpunkwav/breakwater/internal/auth"
	"github.com/daftpunkwav/breakwater/internal/protocol"
)

// TestSetBodyStoresOnce: the first reader owns the read; later calls
// are absorbed instead of overwriting the first ingest.
func TestSetBodyStoresOnce(t *testing.T) {
	t.Parallel()

	carrier := &Carrier{}
	firstChat := protocol.ChatRequest{Model: "m1"}
	carrier.SetBody([]byte("first"), firstChat, []byte("upstream-1"), nil)

	carrier.SetBody([]byte("second"), protocol.ChatRequest{Model: "m2"}, []byte("upstream-2"),
		errors.New("late failure"))

	if !carrier.ChatSet {
		t.Fatal("ChatSet = false, want true after the first write")
	}
	if string(carrier.Body) != "first" || carrier.Chat.Model != "m1" ||
		string(carrier.UpstreamBody) != "upstream-1" {
		t.Fatalf("first write overwritten: body=%q chat=%q upstream=%q",
			carrier.Body, carrier.Chat.Model, carrier.UpstreamBody)
	}
	if carrier.ChatErr != nil {
		t.Fatalf("ChatErr = %v, want nil (the late failure must be absorbed)", carrier.ChatErr)
	}
}

// TestSetBodyCarriesIngestError: the ingest error of the owning read
// is stored for the stages that render it.
func TestSetBodyCarriesIngestError(t *testing.T) {
	t.Parallel()

	carrier := &Carrier{}
	ingestErr := errors.New("model is required")
	carrier.SetBody(nil, protocol.ChatRequest{}, nil, ingestErr)
	if !errors.Is(carrier.ChatErr, ingestErr) {
		t.Fatalf("ChatErr = %v, want the stored ingest error", carrier.ChatErr)
	}
}

// TestCarrierContextRoundTrip: WithCarrier/CarrierFrom attach and
// retrieve the typed carrier; a plain context yields nil.
func TestCarrierContextRoundTrip(t *testing.T) {
	t.Parallel()

	if got := CarrierFrom(context.Background()); got != nil {
		t.Fatalf("CarrierFrom(plain ctx) = %v, want nil", got)
	}

	carrier := &Carrier{Tenant: auth.Tenant{ID: "t1"}}
	ctx := WithCarrier(context.Background(), carrier)
	if got := CarrierFrom(ctx); got != carrier {
		t.Fatalf("CarrierFrom = %p, want the attached carrier %p", got, carrier)
	}
}

// TestRequireCarrierRejectsMissing: without a carrier the misconfigured
// envelope is written and the caller is told to stop.
func TestRequireCarrierRejectsMissing(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)

	carrier, ok := RequireCarrier(rec, req)
	if ok || carrier != nil {
		t.Fatalf("ok=%v carrier=%v, want false/nil without a carrier", ok, carrier)
	}
	assertEnvelope(t, rec, http.StatusInternalServerError, "pipeline_misconfigured")
}

// TestRequireCarrierPassesPresent: a chain-entry carrier passes the
// guard untouched.
func TestRequireCarrierPassesPresent(t *testing.T) {
	t.Parallel()

	carrier := &Carrier{}
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil).
		WithContext(WithCarrier(context.Background(), carrier))
	rec := httptest.NewRecorder()

	got, ok := RequireCarrier(rec, req)
	if !ok || got != carrier {
		t.Fatalf("ok=%v carrier=%p, want true and the request carrier", ok, got)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("nothing should be written, status = %d", rec.Code)
	}
}

// TestCarrierStageAssemblesCarrier: the chain-entry stage attaches a
// fresh carrier to every request it forwards.
func TestCarrierStageAssemblesCarrier(t *testing.T) {
	t.Parallel()

	var seen *Carrier
	handler := CarrierStage()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen = CarrierFrom(r.Context())
	}))

	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", nil))
	if seen == nil {
		t.Fatal("downstream saw no carrier")
	}
}

// TestCarrierStageFreshCarrierPerRequest: two requests never share a
// carrier instance.
func TestCarrierStageFreshCarrierPerRequest(t *testing.T) {
	t.Parallel()

	var first *Carrier
	handler := CarrierStage()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if first == nil {
			first = CarrierFrom(r.Context())
			return
		}
		if CarrierFrom(r.Context()) == first {
			t.Error("second request reused the first request's carrier")
		}
	}))

	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", nil))
	handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodPost, "/", nil))
}
