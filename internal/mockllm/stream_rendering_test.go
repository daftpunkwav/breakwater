/**
 * @file stream_rendering_test
 * @description SSE rendering of the mock upstream: the chunk sequence
 * with usage, the abort mode's truncated stream, slow-mode pacing, and
 * the write-path failure branches of the stream writer.
 */
package mockllm

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestStreamNormalCompletion(t *testing.T) {
	t.Parallel()
	h := New(Options{})
	rec := postCompletion(t, h, nil,
		`{"model":"mock-gpt","stream":true,"messages":[{"role":"user","content":"a b c"}]}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content type = %q, want text/event-stream", ct)
	}

	body := rec.Body.String()
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

// TestStreamAbortMidFlight drives the abort mode over a real server:
// the connection truncates after a few chunks — a read error for the
// client, no [DONE], and delivered partial chunks stay delivered.
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

// TestStreamAbortTruncatesWithoutDone drives the abort mode through the
// writer directly: the generator terminates via http.ErrAbortHandler
// after the second content chunk.
func TestStreamAbortTruncatesWithoutDone(t *testing.T) {
	t.Parallel()
	req := completionRequest{
		Model:    "mock-gpt",
		Stream:   true,
		Messages: []chatMessage{{Role: "user", Content: "hi"}},
	}

	defer func() {
		if r := recover(); r != http.ErrAbortHandler {
			t.Fatalf("abort mode panicked with %v, want http.ErrAbortHandler", r)
		}
	}()
	_ = writeStream(context.Background(), httptest.NewRecorder(), req,
		Faults{StreamMode: StreamModeAbort, CompletionTokens: 4})
}

func TestStreamSlowChunks(t *testing.T) {
	t.Parallel()
	h := New(Options{})
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(
		`{"model":"mock-gpt","stream":true,"messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set(headerStreamMode, "slow")
	req.Header.Set(headerChunkDelay, "40")
	req.Header.Set(headerCompletionTok, "4")

	start := time.Now()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	// Four content chunks, each preceded by a 40ms delay.
	if elapsed := time.Since(start); elapsed < 120*time.Millisecond {
		t.Errorf("elapsed = %v, want at least 120ms of chunk delays", elapsed)
	}
}

// bareWriter hides the recorder's Flush: the type assertion in
// writeStream must fail for it.
type bareWriter struct {
	http.ResponseWriter
}

// TestStreamRequiresFlusher pins the streaming guard: without Flusher
// support chunk delivery cannot be guaranteed, so the writer refuses.
func TestStreamRequiresFlusher(t *testing.T) {
	t.Parallel()
	req := completionRequest{
		Model:    "mock-gpt",
		Stream:   true,
		Messages: []chatMessage{{Role: "user", Content: "hi"}},
	}
	err := writeStream(context.Background(), bareWriter{httptest.NewRecorder()}, req, Faults{})
	if !errors.Is(err, errNoFlusher) {
		t.Fatalf("err = %v, want errNoFlusher", err)
	}
}

// scriptedWriter accepts a fixed number of writes then fails, letting
// the sweep reach every emit error path of writeStream.
type scriptedWriter struct {
	remaining int
}

func (w *scriptedWriter) Header() http.Header { return http.Header{} }

func (w *scriptedWriter) Write(p []byte) (int, error) {
	if w.remaining <= 0 {
		return 0, errors.New("client connection lost")
	}
	w.remaining--
	return len(p), nil
}

func (scriptedWriter) WriteHeader(int) {}

func (scriptedWriter) Flush() {}

// TestStreamWriteFailureStopsRendering sweeps the failure point across
// every frame of a minimal stream (role, content, stop, usage, DONE):
// each position must surface the write error.
func TestStreamWriteFailureStopsRendering(t *testing.T) {
	t.Parallel()
	req := completionRequest{
		Model:    "mock-gpt",
		Stream:   true,
		Messages: []chatMessage{{Role: "user", Content: "hi"}},
	}

	for q := 0; q < 5; q++ {
		err := writeStream(context.Background(), &scriptedWriter{remaining: q}, req,
			Faults{CompletionTokens: 1})
		if err == nil {
			t.Fatalf("quota %d: write failure swallowed", q)
		}
		if !strings.Contains(err.Error(), "client connection lost") {
			t.Fatalf("quota %d: err = %v, want the sink failure", q, err)
		}
	}
}

// TestStreamStopsWhenClientLeavesMidChunks pins the per-chunk
// cancellation: a context cancelled between chunks ends writeStream
// with the context's error.
func TestStreamStopsWhenClientLeavesMidChunks(t *testing.T) {
	t.Parallel()
	req := completionRequest{
		Model:    "mock-gpt",
		Stream:   true,
		Messages: []chatMessage{{Role: "user", Content: "hi"}},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := writeStream(ctx, httptest.NewRecorder(), req,
		Faults{StreamMode: StreamModeSlow, ChunkDelay: time.Second, CompletionTokens: 3})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want DeadlineExceeded", err)
	}
}
