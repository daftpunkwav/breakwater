/**
 * @file completions_test
 * @description Endpoint integration: a request walks route, handler,
 * adapter and a real HTTP upstream — streaming and buffered — and the
 * SSE bytes arrive chunk by chunk.
 */
package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/daftpunkwav/breakwater/internal/circuit"
	"github.com/daftpunkwav/breakwater/internal/pipeline"
	"github.com/daftpunkwav/breakwater/internal/protocol"
	"github.com/daftpunkwav/breakwater/internal/relay"
	"github.com/daftpunkwav/breakwater/internal/retry"
	"github.com/daftpunkwav/breakwater/internal/router"
	"github.com/daftpunkwav/breakwater/internal/upstream"
)

// testUpstreamBackend mimics an OpenAI-compatible provider over real
// HTTP, including chunked SSE delivery.
func testUpstreamBackend(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(testUpstreamHandler(t))
}

// testUpstreamHandler is the mock provider behind testUpstreamBackend,
// exposed so tests can wrap it (e.g. to count the requests that
// actually reach the upstream).
func testUpstreamHandler(t *testing.T) http.Handler {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/chat/completions", func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("upstream read body: %v", err)
			return
		}
		if !strings.Contains(string(body), `"model":"m1"`) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":{"message":"unknown model"}}`))
			return
		}
		if strings.Contains(string(body), `"stream":true`) {
			w.Header().Set("Content-Type", "text/event-stream")
			w.WriteHeader(http.StatusOK)
			flusher := w.(http.Flusher)
			for _, chunk := range []string{
				`data: {"choices":[{"delta":{"role":"assistant"}}]}` + "\n\n",
				`data: {"choices":[{"delta":{"content":"hi"}}]}` + "\n\n",
				`data: {"choices":[],"usage":{"prompt_tokens":2,"completion_tokens":1,"total_tokens":3}}` + "\n\n",
				"data: [DONE]\n\n",
			} {
				if _, err := w.Write([]byte(chunk)); err != nil {
					return
				}
				flusher.Flush()
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"hi"}}],"usage":{"prompt_tokens":2,"completion_tokens":1,"total_tokens":3}}`))
	})
	return mux
}

// inferenceMap wraps one handler under the chat format for the routes.
func inferenceMap(handler http.Handler) map[protocol.Format]http.Handler {
	return map[protocol.Format]http.Handler{protocol.FormatOpenAIChat: handler}
}

func buildEndpoint(t *testing.T, backendURL string) http.Handler {
	t.Helper()
	adapter, err := upstream.NewOpenAI(upstream.OpenAIConfig{
		ID:       "test",
		BaseURL:  backendURL,
		ProbeURL: backendURL + "/healthz",
	})
	if err != nil {
		t.Fatalf("build adapter: %v", err)
	}
	rt, err := router.NewPriority([]router.Binding{{Models: []string{"m1"}, Upstream: adapter}})
	if err != nil {
		t.Fatalf("build router: %v", err)
	}
	relayer := relay.New(retry.Policy{MaxAttempts: 2}, retry.NewBudget(4))
	return pipeline.Chain(
		pipeline.CarrierStage(),
		pipeline.FormatStage(protocol.FormatOpenAIChat),
	)(NewInference(protocol.FormatOpenAIChat, rt, relayer))
}

func TestCompletionsBufferedRoundTrip(t *testing.T) {
	t.Parallel()
	backend := testUpstreamBackend(t)
	defer backend.Close()

	srv := httptest.NewServer(newRootHandler(inferenceMap(buildEndpoint(t, backend.URL)), nil, nil, nil))
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"m1","messages":[{"role":"user","content":"hello"}]}`))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d body = %s", resp.StatusCode, raw)
	}
	if !strings.Contains(string(raw), `"content":"hi"`) {
		t.Fatalf("body = %s, want completion content", raw)
	}
}

func TestCompletionsStreamedRoundTrip(t *testing.T) {
	t.Parallel()
	backend := testUpstreamBackend(t)
	defer backend.Close()

	srv := httptest.NewServer(newRootHandler(inferenceMap(buildEndpoint(t, backend.URL)), nil, nil, nil))
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"m1","stream":true,"messages":[{"role":"user","content":"hello"}]}`))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content type = %q", ct)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	body := string(raw)
	if !strings.Contains(body, `"content":"hi"`) || !strings.HasSuffix(body, "data: [DONE]\n\n") {
		t.Fatalf("stream = %q", body)
	}
}

func TestCompletionsUnknownModel(t *testing.T) {
	t.Parallel()
	backend := testUpstreamBackend(t)
	defer backend.Close()

	srv := httptest.NewServer(newRootHandler(inferenceMap(buildEndpoint(t, backend.URL)), nil, nil, nil))
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"nope","messages":[]}`))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", resp.StatusCode)
	}
}

// TestCompletionsAllUpstreamsCircuitOpenIsUnavailable pins the
// circuit-open error contract at the routing boundary: a model with
// bindings whose every upstream is breaker-open is a fast 503 with the
// circuit_open code — the same envelope the executor renders when the
// breaker denies an attempt — never a misleading 404.
func TestCompletionsAllUpstreamsCircuitOpenIsUnavailable(t *testing.T) {
	t.Parallel()
	backend := testUpstreamBackend(t)
	defer backend.Close()

	adapter, err := upstream.NewOpenAI(upstream.OpenAIConfig{ID: "test", BaseURL: backend.URL})
	if err != nil {
		t.Fatalf("build adapter: %v", err)
	}
	breaker := circuit.NewRegistry(circuit.Config{FailThreshold: 1, Cooldown: time.Hour})
	perm, ok := breaker.Allow(context.Background(), "test")
	if !ok {
		t.Fatal("breaker denied in closed state")
	}
	perm.Report(circuit.OutcomeServerFault)

	rt, err := router.NewPriority([]router.Binding{{Models: []string{"m1"}, Upstream: adapter}},
		router.WithBreaker(breaker))
	if err != nil {
		t.Fatalf("build router: %v", err)
	}
	relayer := relay.New(retry.Policy{MaxAttempts: 2}, retry.NewBudget(4), relay.WithBreaker(breaker))
	handler := pipeline.Chain(
		pipeline.CarrierStage(),
		pipeline.FormatStage(protocol.FormatOpenAIChat),
	)(NewInference(protocol.FormatOpenAIChat, rt, relayer))

	srv := httptest.NewServer(newRootHandler(inferenceMap(handler), nil, nil, nil))
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"m1","messages":[{"role":"user","content":"hello"}]}`))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusServiceUnavailable || !strings.Contains(string(raw), `"code":"circuit_open"`) {
		t.Fatalf("status = %d body = %s, want 503 circuit_open envelope", resp.StatusCode, raw)
	}
}

func TestCompletionsMalformedBody(t *testing.T) {
	t.Parallel()
	backend := testUpstreamBackend(t)
	defer backend.Close()

	srv := httptest.NewServer(newRootHandler(inferenceMap(buildEndpoint(t, backend.URL)), nil, nil, nil))
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json", strings.NewReader("{not json"))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}
