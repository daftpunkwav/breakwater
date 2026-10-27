/**
 * @file inference_rejectcode_test
 * @description The inference handler's own rejections ride the failure
 * taxonomy: every code the handler renders is stamped on the carrier's
 * rejection slot, so the access log records the gateway cause instead
 * of an empty code the stability report would read as an upstream
 * error passthrough of the status class.
 */
package server

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/daftpunkwav/breakwater/internal/obs"
	"github.com/daftpunkwav/breakwater/internal/pipeline"
	"github.com/daftpunkwav/breakwater/internal/protocol"
	"github.com/daftpunkwav/breakwater/internal/relay"
	"github.com/daftpunkwav/breakwater/internal/retry"
	"github.com/daftpunkwav/breakwater/internal/router"
	"github.com/daftpunkwav/breakwater/internal/upstream"
)

// captureSink collects the access entries the observation stage emits.
type captureSink struct{ entries []obs.Entry }

func (s *captureSink) Record(entry obs.Entry) { s.entries = append(s.entries, entry) }

func (s *captureSink) Flush(context.Context) error { return nil }

// TestInferenceRejectionsCarryTheirCode drives the four handler-owned
// rejections through the observation stage and asserts the status, the
// rendered envelope code and the access entry all name the same gateway
// cause.
func TestInferenceRejectionsCarryTheirCode(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		router     router.Router
		limits     map[string]int64
		chat       protocol.ChatRequest
		wantStatus int
		wantCode   string
	}{
		{
			name:       "model disabled by an operator",
			router:     stubRouter{err: router.ErrDisabled},
			chat:       protocol.ChatRequest{Model: "m1", Messages: longPrompt()},
			wantStatus: http.StatusForbidden,
			wantCode:   "model_disabled",
		},
		{
			name:       "every upstream unavailable",
			router:     stubRouter{err: router.ErrUnavailable},
			chat:       protocol.ChatRequest{Model: "m1", Messages: longPrompt()},
			wantStatus: http.StatusServiceUnavailable,
			wantCode:   "circuit_open",
		},
		{
			name:       "no upstream serves the model",
			router:     stubRouter{err: fmt.Errorf("no upstream serves model %q", "m1")},
			chat:       protocol.ChatRequest{Model: "m1", Messages: longPrompt()},
			wantStatus: http.StatusNotFound,
			wantCode:   "model_not_found",
		},
		{
			name:       "prompt exceeds the context ceiling",
			router:     stubRouter{candidates: []upstream.Upstream{usageLessUpstream{}}},
			limits:     map[string]int64{"m1": 4},
			chat:       protocol.ChatRequest{Model: "m1", Messages: longPrompt()},
			wantStatus: http.StatusRequestEntityTooLarge,
			wantCode:   "context_window_exceeded",
		},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			sink := &captureSink{}
			handler := pipeline.Chain(
				pipeline.CarrierStage(),
				pipeline.RequestIDStage(),
				pipeline.FormatStage(protocol.FormatOpenAIChat),
				pipeline.ObservationStage(nil, sink),
				pipeline.RecoveryStage(),
			)(NewInference(protocol.FormatOpenAIChat, tc.router,
				relay.New(retry.Policy{MaxAttempts: 1}, nil), WithContextLimits(tc.limits)))

			rec := inferenceRequest(t, handler, tc.chat)
			if rec.Code != tc.wantStatus || !strings.Contains(rec.Body.String(), `"code":"`+tc.wantCode+`"`) {
				t.Fatalf("status = %d body = %s, want %d with code %q",
					rec.Code, rec.Body.String(), tc.wantStatus, tc.wantCode)
			}
			if len(sink.entries) != 1 {
				t.Fatalf("observed %d access entries, want 1", len(sink.entries))
			}
			if got := sink.entries[0].ErrorCode; got != tc.wantCode {
				t.Fatalf("access entry error code = %q, want %q", got, tc.wantCode)
			}
		})
	}
}
