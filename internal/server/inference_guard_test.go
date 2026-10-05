/**
 * @file inference_guard_test
 * @description Inference handler guards and settlement fallback: the
 * missing-carrier misconfiguration, the model-presence rule, and the
 * reservation-based consumption for replies that carried no usage.
 */
package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/daftpunkwav/breakwater/internal/auth"
	"github.com/daftpunkwav/breakwater/internal/pipeline"
	"github.com/daftpunkwav/breakwater/internal/protocol"
	"github.com/daftpunkwav/breakwater/internal/relay"
	"github.com/daftpunkwav/breakwater/internal/retry"
	"github.com/daftpunkwav/breakwater/internal/upstream"
)

// stubRouter answers Candidates from a fixed candidate list.
type stubRouter struct {
	candidates []upstream.Upstream
	err        error
}

func (s stubRouter) Candidates(context.Context, string) ([]upstream.Upstream, error) {
	return s.candidates, s.err
}

// usageLessUpstream answers a 200 reply that carries no usage object.
type usageLessUpstream struct{}

func (usageLessUpstream) ID() string { return "raw" }

func (usageLessUpstream) Forward(context.Context, upstream.Request) (*upstream.Response, error) {
	header := http.Header{}
	header.Set("Content-Type", "application/json")
	return &upstream.Response{
		StatusCode: http.StatusOK,
		Header:     header,
		Body:       io.NopCloser(strings.NewReader(`{"choices":[{"message":{"content":"hi"}}]}`)),
	}, nil
}

func (usageLessUpstream) Probe(context.Context) error { return nil }

// chatHandler builds the inference handler over the given candidates;
// callers attach their own carrier via pipeline.WithCarrier.
func chatHandler(t *testing.T, candidates []upstream.Upstream) http.Handler {
	t.Helper()
	relayer := relay.New(retry.Policy{MaxAttempts: 1}, nil)
	return NewInference(protocol.FormatOpenAIChat, stubRouter{candidates: candidates}, relayer)
}

// postWithCarrier fires one POST with the carrier attached to its
// context and returns the recorder.
func postWithCarrier(t *testing.T, handler http.Handler, carrier *pipeline.Carrier, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req = req.WithContext(pipeline.WithCarrier(req.Context(), carrier))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func TestInferenceWithoutCarrierRendersMisconfigured(t *testing.T) {
	t.Parallel()
	handler := NewInference(protocol.FormatOpenAIChat, stubRouter{}, relay.New(retry.Policy{MaxAttempts: 1}, nil))

	rec := postWithCarrier(t, handler, nil, `{"model":"m1","messages":[]}`)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "pipeline_misconfigured") {
		t.Fatalf("body = %s, want the misconfiguration envelope", rec.Body.String())
	}
}

func TestInferenceRejectsMissingModel(t *testing.T) {
	t.Parallel()
	handler := chatHandler(t, []upstream.Upstream{usageLessUpstream{}})
	carrier := &pipeline.Carrier{Format: protocol.FormatOpenAIChat}

	rec := postWithCarrier(t, handler, carrier, `{"messages":[{"role":"user","content":"hi"}]}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "model is required") {
		t.Fatalf("body = %s, want the model-presence rejection", rec.Body.String())
	}
}

// TestInferenceDeniesModelOutsideTier pins the handler's own tier check:
// in the ungoverned assembly (no pipeline authz stage in front — the
// no-identity deployment mode) the inference handler is the last line
// of the fail-closed model rule. When the stage IS installed, it
// rejects first and this branch is defense in depth.
func TestInferenceDeniesModelOutsideTier(t *testing.T) {
	t.Parallel()
	handler := chatHandler(t, []upstream.Upstream{usageLessUpstream{}})
	carrier := &pipeline.Carrier{
		Format: protocol.FormatOpenAIChat,
		Tenant: auth.Tenant{ID: "t1", Tier: auth.Tier{AllowedModels: []string{"m1"}}},
	}

	rec := postWithCarrier(t, handler, carrier, `{"model":"m2","messages":[{"role":"user","content":"hi"}]}`)
	if rec.Code != http.StatusForbidden || !strings.Contains(rec.Body.String(), "model_not_allowed") {
		t.Fatalf("status = %d body = %s, want 403 model_not_allowed", rec.Code, rec.Body.String())
	}
}

// TestInferenceSettlesByReservationWithoutUsage pins the last settlement
// fallback: a delivered 2xx reply without a usage object consumes the
// reservation estimate.
func TestInferenceSettlesByReservationWithoutUsage(t *testing.T) {
	t.Parallel()
	carrier := &pipeline.Carrier{Format: protocol.FormatOpenAIChat, Tokens: 51}
	handler := chatHandler(t, []upstream.Upstream{usageLessUpstream{}})

	rec := postWithCarrier(t, handler, carrier, `{"model":"m1","messages":[{"role":"user","content":"hello"}]}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s, want 200", rec.Code, rec.Body.String())
	}
	if carrier.Relay == nil {
		t.Fatal("relay outcome not captured on the carrier")
	}
	if carrier.Relay.UsageKnown {
		t.Fatal("unexpected usage extraction on a usage-less reply")
	}
	if carrier.Consumed != 51 {
		t.Fatalf("consumed = %d, want the reservation 51", carrier.Consumed)
	}
}
