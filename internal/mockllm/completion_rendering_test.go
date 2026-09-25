/**
 * @file completion_rendering_test
 * @description Buffered completion rendering: the OpenAI response
 * shape, deterministic token accounting, injected statuses and delays,
 * and the process-wide error rate.
 */
package mockllm

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestNonStreamCompletion(t *testing.T) {
	t.Parallel()
	h := New(Options{})
	rec := postCompletion(t, h, nil,
		`{"model":"mock-gpt","messages":[{"role":"user","content":"hello world"}]}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	var body completionResponse
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if body.Object != "chat.completion" {
		t.Errorf("object = %q, want chat.completion", body.Object)
	}
	if body.Model != "mock-gpt" {
		t.Errorf("model = %q, want mock-gpt", body.Model)
	}
	if len(body.Choices) != 1 {
		t.Fatalf("choices = %d, want 1", len(body.Choices))
	}
	if got := len(strings.Fields(body.Choices[0].Message.Content)); got != defaultCompletionTokens {
		t.Errorf("content tokens = %d, want %d", got, defaultCompletionTokens)
	}
	if body.Usage == nil {
		t.Fatal("usage missing")
	}
	if body.Usage.PromptTokens != 2 {
		t.Errorf("prompt tokens = %d, want 2", body.Usage.PromptTokens)
	}
	if body.Usage.CompletionTokens != defaultCompletionTokens {
		t.Errorf("completion tokens = %d, want %d", body.Usage.CompletionTokens, defaultCompletionTokens)
	}
	if body.Usage.TotalTokens != body.Usage.PromptTokens+body.Usage.CompletionTokens {
		t.Errorf("total tokens = %d, want prompt+completion", body.Usage.TotalTokens)
	}
}

// TestPromptTokensCountsWordsAcrossMessages pins the prompt meter: one
// token per whitespace-separated word over all messages, with a floor
// of one for an empty conversation.
func TestPromptTokensCountsWordsAcrossMessages(t *testing.T) {
	t.Parallel()
	req := completionRequest{Messages: []chatMessage{
		{Role: "user", Content: "one two three"},
		{Role: "assistant", Content: "four"},
	}}
	if got := promptTokens(req); got != 4 {
		t.Fatalf("prompt tokens = %d, want 4", got)
	}
	if got := promptTokens(completionRequest{}); got != 1 {
		t.Fatalf("empty conversation = %d, want the floor of 1", got)
	}
}

func TestInjectedStatus(t *testing.T) {
	t.Parallel()
	h := New(Options{})
	rec := postCompletion(t, h, map[string]string{headerStatus: "503"},
		`{"model":"mock-gpt","messages":[{"role":"user","content":"hi"}]}`)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rec.Code)
	}

	var body errorEnvelope
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatalf("decode error envelope: %v", err)
	}
	if body.Error.Code != "injected_status" {
		t.Errorf("code = %q, want injected_status", body.Error.Code)
	}
}

func TestInjectedDelay(t *testing.T) {
	t.Parallel()
	h := New(Options{DefaultDelay: 100 * time.Millisecond})

	start := time.Now()
	rec := postCompletion(t, h, map[string]string{headerDelay: "80"},
		`{"model":"mock-gpt","messages":[{"role":"user","content":"hi"}]}`)
	elapsed := time.Since(start)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	// Default delay and the per-request directive add up.
	if elapsed < 150*time.Millisecond {
		t.Errorf("elapsed = %v, want at least the combined delay", elapsed)
	}
}

func TestGlobalErrorRate(t *testing.T) {
	t.Parallel()
	h := New(Options{ErrorRate: 1})
	rec := postCompletion(t, h, nil,
		`{"model":"mock-gpt","messages":[{"role":"user","content":"hi"}]}`)
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "injected_failure") {
		t.Fatalf("body = %s, want the injected failure envelope", rec.Body.String())
	}
}

// TestNewClampsInvalidOptions pins the Options guard rails: negative
// delays and rates clamp to zero, rates above one clamp to one.
func TestNewClampsInvalidOptions(t *testing.T) {
	t.Parallel()
	h := New(Options{DefaultDelay: -time.Second, ErrorRate: -0.5})
	if h.opts.DefaultDelay != 0 || h.opts.ErrorRate != 0 {
		t.Fatalf("negative clamps = %+v, want zeroed", h.opts)
	}

	h = New(Options{ErrorRate: 1.5})
	if h.opts.ErrorRate != 1 {
		t.Fatalf("error rate = %v, want clamped to 1", h.opts.ErrorRate)
	}
}

// TestCompletionCancelledDuringDelay pins the disconnect path: a client
// that walks away during the injected delay ends the handler silently.
func TestCompletionCancelledDuringDelay(t *testing.T) {
	t.Parallel()
	h := New(Options{DefaultDelay: time.Hour})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"mock-gpt","messages":[{"role":"user","content":"hi"}]}`))
	req = req.WithContext(ctx)
	rec := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		defer close(done)
		h.ServeHTTP(rec, req)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handler ignored the client disconnect during the delay")
	}
}
