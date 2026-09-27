/**
 * @file inference_context_test
 * @description The model-level context pre-filter and the fallback
 * chain as wired through the inference handler: over-ceiling prompts
 * refuse before any attempt, fitting prompts pass, and a drained
 * primary model hands the request to the chain.
 */
package server

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/daftpunkwav/breakwater/internal/pipeline"
	"github.com/daftpunkwav/breakwater/internal/protocol"
	"github.com/daftpunkwav/breakwater/internal/relay"
	"github.com/daftpunkwav/breakwater/internal/retry"
	"github.com/daftpunkwav/breakwater/internal/upstream"
)

// modelRouter answers Candidates per model, like the priority router
// does; an unknown model is a no-binding failure.
type modelRouter struct {
	byModel map[string][]upstream.Upstream
}

func (m modelRouter) Candidates(_ context.Context, model string) ([]upstream.Upstream, error) {
	cands, ok := m.byModel[model]
	if !ok {
		return nil, fmt.Errorf("no upstream serves model %q", model)
	}
	return cands, nil
}

// scriptedInferenceUpstream answers with a fixed status and records
// the model of each exchange.
type scriptedInferenceUpstream struct {
	id     string
	status int
	models []string
}

func (s *scriptedInferenceUpstream) ID() string { return s.id }

func (s *scriptedInferenceUpstream) Forward(_ context.Context, req upstream.Request) (*upstream.Response, error) {
	s.models = append(s.models, req.Model)
	header := http.Header{}
	header.Set("Content-Type", "application/json")
	body := `{"error":{"type":"server_error"}}`
	if s.status >= 200 && s.status < 300 {
		body = `{"choices":[{"message":{"content":"hi"}}]}`
	}
	return &upstream.Response{
		StatusCode: s.status,
		Header:     header,
		Body:       io.NopCloser(strings.NewReader(body)),
	}, nil
}

func (s *scriptedInferenceUpstream) Probe(context.Context) error { return nil }

// inferenceRequest builds a request whose JSON body is the given chat
// request and attaches an empty carrier, so the handler's own ingest
// (AuthorizeModel → EnsureBody) parses the body the way the real
// pipeline does — a manually prefilled Chat would be overwritten by
// that same ingest.
func inferenceRequest(t *testing.T, handler http.Handler, chat protocol.ChatRequest) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(chat)
	if err != nil {
		t.Fatalf("marshal chat: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(raw))
	req = req.WithContext(pipeline.WithCarrier(req.Context(),
		&pipeline.Carrier{Format: protocol.FormatOpenAIChat}))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}

func longPrompt() []protocol.ChatMessage {
	return []protocol.ChatMessage{{Role: "user", Content: "one two three four five"}}
}

func TestInferenceFiltersContextExceedingModel(t *testing.T) {
	t.Parallel()
	up := &usageLessUpstream{}
	handler := NewInference(protocol.FormatOpenAIChat, stubRouter{candidates: []upstream.Upstream{up}},
		relay.New(retry.Policy{MaxAttempts: 1}, nil),
		WithContextLimits(map[string]int64{"m1": 4}))

	rec := inferenceRequest(t, handler, protocol.ChatRequest{Model: "m1", Messages: longPrompt()})
	if rec.Code != http.StatusRequestEntityTooLarge || !strings.Contains(rec.Body.String(), "context_window_exceeded") {
		t.Fatalf("status = %d body = %s, want 413 context_window_exceeded", rec.Code, rec.Body.String())
	}
}

func TestInferenceKeepsFittingAndUnlimitedModels(t *testing.T) {
	t.Parallel()
	up := &usageLessUpstream{}
	limited := NewInference(protocol.FormatOpenAIChat, stubRouter{candidates: []upstream.Upstream{up}},
		relay.New(retry.Policy{MaxAttempts: 1}, nil),
		WithContextLimits(map[string]int64{"m1": 5}))
	rec := inferenceRequest(t, limited, protocol.ChatRequest{Model: "m1", Messages: longPrompt()})
	if rec.Code != http.StatusOK {
		t.Fatalf("fitting prompt status = %d body = %s, want 200", rec.Code, rec.Body.String())
	}

	unlimited := NewInference(protocol.FormatOpenAIChat, stubRouter{candidates: []upstream.Upstream{up}},
		relay.New(retry.Policy{MaxAttempts: 1}, nil))
	rec = inferenceRequest(t, unlimited, protocol.ChatRequest{Model: "other", Messages: longPrompt()})
	if rec.Code != http.StatusOK {
		t.Fatalf("unlimited model status = %d body = %s, want 200", rec.Code, rec.Body.String())
	}
}

func TestInferenceFallsBackAcrossModels(t *testing.T) {
	t.Parallel()
	failing := &scriptedInferenceUpstream{id: "u1", status: http.StatusServiceUnavailable}
	healthy := &scriptedInferenceUpstream{id: "u2", status: http.StatusOK}
	handler := NewInference(protocol.FormatOpenAIChat,
		modelRouter{byModel: map[string][]upstream.Upstream{"m1": {failing}, "m2": {healthy}}},
		relay.New(retry.Policy{MaxAttempts: 2}, nil),
		WithFallbacks(map[string][]string{"m1": {"m2"}}))

	rec := inferenceRequest(t, handler, protocol.ChatRequest{Model: "m1", Messages: longPrompt()})
	if rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), "hi") {
		t.Fatalf("status = %d body = %s, want 200 served by the fallback model", rec.Code, rec.Body.String())
	}
	if len(failing.models) != 1 || failing.models[0] != "m1" {
		t.Fatalf("primary exchanges = %v, want one for m1", failing.models)
	}
	if len(healthy.models) != 1 || healthy.models[0] != "m2" {
		t.Fatalf("fallback exchanges = %v, want one for m2: the body's model must ride the chain", healthy.models)
	}
}

// TestInferenceResolverFiltersFallbacks: the context ceiling applies
// to the fallback hop too — an over-ceiling fallback is skipped, the
// chain drains, and the last primary candidate takes the extra attempt.
func TestInferenceResolverFiltersFallbacks(t *testing.T) {
	t.Parallel()
	failing := &scriptedInferenceUpstream{id: "u1", status: http.StatusServiceUnavailable}
	handler := NewInference(protocol.FormatOpenAIChat,
		modelRouter{byModel: map[string][]upstream.Upstream{"m1": {failing}, "m2": {&usageLessUpstream{}}}},
		relay.New(retry.Policy{MaxAttempts: 2}, nil),
		WithFallbacks(map[string][]string{"m1": {"m2"}}),
		WithContextLimits(map[string]int64{"m2": 4}))

	rec := inferenceRequest(t, handler, protocol.ChatRequest{Model: "m1", Messages: longPrompt()})
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d body = %s, want the 503 passthrough after the filtered chain drains", rec.Code, rec.Body.String())
	}
	if len(failing.models) != 2 {
		t.Fatalf("primary exchanges = %d, want 2 (both attempts clamp onto u1)", len(failing.models))
	}
}
