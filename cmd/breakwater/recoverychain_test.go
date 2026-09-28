/**
 * @file recoverychain_test
 * @description The assembled inference chain's panic containment order,
 * locked at the composition root: the recovery stage sits inside the
 * observation stage the governance list opens with, so a handler panic
 * renders as a counted 500 gateway fault — never as the client
 * disconnect (status 499) a header-less end would read as.
 */
package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/daftpunkwav/breakwater/internal/obs"
	"github.com/daftpunkwav/breakwater/internal/pipeline"
	"github.com/daftpunkwav/breakwater/internal/protocol"
)

// TestInferenceChainCountsPanicAsGatewayFault pins the recovery order:
// observation wraps recovery, so a panic's rendered 500 lands in the
// request counter and the access entry instead of masquerading as a
// disconnect.
func TestInferenceChainCountsPanicAsGatewayFault(t *testing.T) {
	metrics := obs.NewMetrics()
	sink := &countingSink{}
	// The governance list mirrors the real assembly's head: observation
	// is its first stage (serve.go), which is what makes the panic
	// observable at all.
	governance := []pipeline.Middleware{pipeline.ObservationStage(metrics, sink)}
	panicking := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		panic("endpoint exploded")
	})

	handler := inferenceChain(protocol.FormatOpenAIChat, governance, panicking)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil))

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want the recovery stage's rendered 500", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "internal_error") {
		t.Fatalf("body = %q, want the internal_error envelope", rec.Body.String())
	}

	var out strings.Builder
	if err := metrics.Render(&out); err != nil {
		t.Fatalf("render metrics: %v", err)
	}
	if !strings.Contains(out.String(),
		`breakwater_requests_total{tenant="",model="",upstream="-",status="500"} 1`) {
		t.Fatalf("panic not counted as a 500 gateway fault: %s", out.String())
	}
	if len(sink.entries) != 1 || sink.entries[0].Status != http.StatusInternalServerError {
		t.Fatalf("sink entries = %+v, want one entry at 500 (never %d)",
			sink.entries, obs.StatusClientClosedRequest)
	}
}
