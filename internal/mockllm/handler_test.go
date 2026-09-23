/**
 * @file handler_test
 * @description Behavior tests for the mock upstream: directive parsing,
 * happy-path rendering and every injected fault mode.
 */
package mockllm

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestParseFaults(t *testing.T) {
	t.Parallel()

	t.Run("defaults when no headers present", func(t *testing.T) {
		t.Parallel()
		f, err := parseFaults(http.Header{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if f.StreamMode != StreamModeNormal {
			t.Errorf("stream mode = %q, want %q", f.StreamMode, StreamModeNormal)
		}
		if f.Delay != 0 || f.Status != 0 || f.CompletionTokens != 0 {
			t.Errorf("expected zero-valued directives, got %+v", f)
		}
	})

	t.Run("full valid set", func(t *testing.T) {
		t.Parallel()
		h := http.Header{}
		h.Set(headerDelay, "250")
		h.Set(headerStatus, "503")
		h.Set(headerStreamMode, "slow")
		h.Set(headerChunkDelay, "40")
		h.Set(headerOmitUsage, "true")
		h.Set(headerCompletionTok, "8")
		f, err := parseFaults(h)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if f.Delay != 250*time.Millisecond {
			t.Errorf("delay = %v, want 250ms", f.Delay)
		}
		if f.Status != 503 {
			t.Errorf("status = %d, want 503", f.Status)
		}
		if f.StreamMode != StreamModeSlow {
			t.Errorf("stream mode = %q, want %q", f.StreamMode, StreamModeSlow)
		}
		if f.ChunkDelay != 40*time.Millisecond {
			t.Errorf("chunk delay = %v, want 40ms", f.ChunkDelay)
		}
		if !f.OmitUsage {
			t.Error("omit usage = false, want true")
		}
		if f.CompletionTokens != 8 {
			t.Errorf("completion tokens = %d, want 8", f.CompletionTokens)
		}
	})

	t.Run("slow mode gets default chunk delay", func(t *testing.T) {
		t.Parallel()
		h := http.Header{}
		h.Set(headerStreamMode, "slow")
		f, err := parseFaults(h)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if f.ChunkDelay != defaultChunkDelay {
			t.Errorf("chunk delay = %v, want %v", f.ChunkDelay, defaultChunkDelay)
		}
	})

	t.Run("rejects unknown stream mode", func(t *testing.T) {
		t.Parallel()
		h := http.Header{}
		h.Set(headerStreamMode, "explode")
		if _, err := parseFaults(h); err == nil {
			t.Fatal("expected error for unknown stream mode")
		}
	})

	t.Run("rejects non-error status", func(t *testing.T) {
		t.Parallel()
		h := http.Header{}
		h.Set(headerStatus, "200")
		if _, err := parseFaults(h); err == nil {
			t.Fatal("expected error for non-error status")
		}
	})

	t.Run("rejects negative delay", func(t *testing.T) {
		t.Parallel()
		h := http.Header{}
		h.Set(headerDelay, "-5")
		if _, err := parseFaults(h); err == nil {
			t.Fatal("expected error for negative delay")
		}
	})

	t.Run("rejects oversized completion", func(t *testing.T) {
		t.Parallel()
		h := http.Header{}
		h.Set(headerCompletionTok, "99999999")
		if _, err := parseFaults(h); err == nil {
			t.Fatal("expected error for oversized completion")
		}
	})
}

func TestNonStreamCompletion(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(New(Options{}))
	t.Cleanup(srv.Close)

	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"mock-gpt","messages":[{"role":"user","content":"hello world"}]}`))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body completionResponse
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
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
	content := body.Choices[0].Message.Content
	if got := len(strings.Fields(content)); got != defaultCompletionTokens {
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

func TestInjectedStatus(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(New(Options{}))
	t.Cleanup(srv.Close)

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"mock-gpt","messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set(headerStatus, "503")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })

	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}
	var body errorEnvelope
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode error envelope: %v", err)
	}
	if body.Error.Code != "injected_status" {
		t.Errorf("code = %q, want injected_status", body.Error.Code)
	}
}

func TestInjectedDelay(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(New(Options{}))
	t.Cleanup(srv.Close)

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"mock-gpt","messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set(headerDelay, "150")
	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })

	if elapsed := time.Since(start); elapsed < 150*time.Millisecond {
		t.Errorf("elapsed = %v, want at least 150ms", elapsed)
	}
}

func TestGlobalErrorRate(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(New(Options{ErrorRate: 1}))
	t.Cleanup(srv.Close)

	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"mock-gpt","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })

	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", resp.StatusCode)
	}
}

func TestStreamNormalCompletion(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(New(Options{}))
	t.Cleanup(srv.Close)

	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"mock-gpt","stream":true,"messages":[{"role":"user","content":"a b c"}]}`))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })

	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content type = %q, want text/event-stream", ct)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	body := string(raw)
	if !strings.Contains(body, "data: [DONE]") {
		t.Fatal("stream missing [DONE] terminator")
	}

	var (
		contentWords int
		sawStop      bool
		usage        *usage
	)
	for _, event := range strings.Split(body, "\n\n") {
		data, ok := strings.CutPrefix(event, "data: ")
		if !ok || data == "[DONE]" {
			continue
		}
		var chunk chatChunk
		if err := json.Unmarshal([]byte(data), &chunk); err != nil {
			t.Fatalf("unmarshal chunk %q: %v", data, err)
		}
		for _, choice := range chunk.Choices {
			if w := strings.TrimSpace(choice.Delta.Content); w != "" {
				contentWords++
			}
			if choice.FinishReason != nil && *choice.FinishReason == "stop" {
				sawStop = true
			}
		}
		if chunk.Usage != nil {
			usage = chunk.Usage
		}
	}
	if contentWords != defaultCompletionTokens {
		t.Errorf("content chunks = %d, want %d", contentWords, defaultCompletionTokens)
	}
	if !sawStop {
		t.Error("no chunk carried finish_reason stop")
	}
	if usage == nil {
		t.Fatal("usage chunk missing")
	}
	if usage.PromptTokens != 3 {
		t.Errorf("prompt tokens = %d, want 3", usage.PromptTokens)
	}
	if usage.CompletionTokens != defaultCompletionTokens {
		t.Errorf("completion tokens = %d, want %d", usage.CompletionTokens, defaultCompletionTokens)
	}
}

func TestStreamAbortMidFlight(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(New(Options{}))
	t.Cleanup(srv.Close)

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"mock-gpt","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set(headerStreamMode, "abort")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	raw, readErr := io.ReadAll(resp.Body)
	_ = resp.Body.Close()

	if readErr == nil {
		t.Fatal("expected read error from aborted stream")
	}
	if strings.Contains(string(raw), "[DONE]") {
		t.Error("aborted stream must not end with [DONE]")
	}
	if !strings.Contains(string(raw), "data: ") {
		t.Error("aborted stream should have delivered partial chunks")
	}
}

func TestStreamSlowChunks(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(New(Options{}))
	t.Cleanup(srv.Close)

	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/v1/chat/completions", strings.NewReader(
		`{"model":"mock-gpt","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set(headerStreamMode, "slow")
	req.Header.Set(headerChunkDelay, "40")
	req.Header.Set(headerCompletionTok, "4")

	start := time.Now()
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	_, err = io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatalf("read stream: %v", err)
	}
	// Four content chunks, each preceded by a 40ms delay.
	if elapsed := time.Since(start); elapsed < 120*time.Millisecond {
		t.Errorf("elapsed = %v, want at least 120ms of chunk delays", elapsed)
	}
}

func TestRouting(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(New(Options{}))
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/nope")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("unknown path status = %d, want 404", resp.StatusCode)
	}

	resp, err = http.Get(srv.URL + "/v1/chat/completions")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Errorf("GET completions status = %d, want 405", resp.StatusCode)
	}

	resp, err = http.Post(srv.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{not json}`))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("malformed body status = %d, want 400", resp.StatusCode)
	}
}

func TestBodyTooLarge(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(New(Options{}))
	t.Cleanup(srv.Close)

	huge := strings.Repeat("a", maxRequestBytes+1024)
	body := `{"model":"mock-gpt","messages":[{"role":"user","content":"` + huge + `"}]}`
	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", resp.StatusCode)
	}
}
