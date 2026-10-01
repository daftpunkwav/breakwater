/**
 * @file inference_affinity_test
 * @description Prompt-prefix affinity as wired through the inference
 * handler: the upstream holding the recorded prefix is attempted
 * first, and a cold index leaves the router's order standing.
 */
package server

import (
	"testing"
	"time"

	"github.com/daftpunkwav/breakwater/internal/affinity"
	"github.com/daftpunkwav/breakwater/internal/pipeline"
	"github.com/daftpunkwav/breakwater/internal/protocol"
	"github.com/daftpunkwav/breakwater/internal/relay"
	"github.com/daftpunkwav/breakwater/internal/retry"
	"github.com/daftpunkwav/breakwater/internal/upstream"
)

func TestInferencePromotesThePrefixHoldingUpstream(t *testing.T) {
	idx := affinity.NewIndex(time.Minute, nil)
	warm := &scriptedInferenceUpstream{id: "warm", status: 200}
	cold := &scriptedInferenceUpstream{id: "cold", status: 200}
	router := modelRouter{byModel: map[string][]upstream.Upstream{
		"m1": {cold, warm}, // the router prefers cold
	}}
	handler := NewInference(protocol.FormatOpenAIChat, router,
		relay.New(retry.Policy{MaxAttempts: 1}, nil),
		WithAffinity(idx))

	// Seed the index the way any prior request would have: the pick
	// records the head it was given — warm, via a candidate list that
	// put warm first.
	chat := protocol.ChatRequest{Model: "m1", Messages: longPrompt()}
	idx.Pick("m1", pipeline.PromptText(chat), []string{"warm", "cold"})

	rec := inferenceRequest(t, handler, chat)
	if rec.Code != 200 {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	// The relay must have attempted warm first — one exchange, on the
	// upstream the router ordered second.
	if len(warm.models) != 1 {
		t.Fatalf("warm attempts = %d, want 1", len(warm.models))
	}
	if len(cold.models) != 0 {
		t.Fatalf("cold attempts = %d, want 0", len(cold.models))
	}
}

func TestInferenceColdAffinityKeepsRouterOrder(t *testing.T) {
	idx := affinity.NewIndex(time.Minute, nil)
	first := &scriptedInferenceUpstream{id: "first", status: 200}
	second := &scriptedInferenceUpstream{id: "second", status: 200}
	router := modelRouter{byModel: map[string][]upstream.Upstream{
		"m1": {first, second},
	}}
	handler := NewInference(protocol.FormatOpenAIChat, router,
		relay.New(retry.Policy{MaxAttempts: 1}, nil),
		WithAffinity(idx))

	// Nothing recorded: the router's order stands and the pick seeds
	// the head for next time.
	rec := inferenceRequest(t, handler,
		protocol.ChatRequest{Model: "m1", Messages: longPrompt()})
	if rec.Code != 200 {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if len(first.models) != 1 || len(second.models) != 0 {
		t.Fatalf("attempts first = %d second = %d, want 1/0",
			len(first.models), len(second.models))
	}
}

func TestInferenceNilAffinityKeepsRouterOrder(t *testing.T) {
	up := &scriptedInferenceUpstream{id: "only", status: 200}
	router := modelRouter{byModel: map[string][]upstream.Upstream{
		"m1": {up},
	}}
	handler := NewInference(protocol.FormatOpenAIChat, router,
		relay.New(retry.Policy{MaxAttempts: 1}, nil))

	rec := inferenceRequest(t, handler,
		protocol.ChatRequest{Model: "m1", Messages: longPrompt()})
	if rec.Code != 200 || len(up.models) != 1 {
		t.Fatalf("status = %d attempts = %d, want 200/1", rec.Code, len(up.models))
	}
}
