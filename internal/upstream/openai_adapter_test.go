/**
 * @file openai_adapter_test
 * @description The OpenAI-compatible adapter: NewOpenAI validation,
 * the single Forward exchange (path, headers, body, result convention)
 * and the Probe contract.
 */
package upstream

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// newTestAdapter builds an adapter against an httptest server whose
// behavior the test controls, and returns both.
func newTestAdapter(t *testing.T, mutate func(*httptest.Server, *OpenAIConfig)) (*httptest.Server, *OpenAI) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(server.Close)

	cfg := OpenAIConfig{ID: "up-1", BaseURL: server.URL}
	if mutate != nil {
		mutate(server, &cfg)
	}
	adapter, err := NewOpenAI(cfg)
	if err != nil {
		t.Fatalf("NewOpenAI: %v", err)
	}
	return server, adapter
}

// TestNewOpenAIRejectsMisconfiguration: assembly fails without an id
// or base url, before any request exists.
func TestNewOpenAIRejectsMisconfiguration(t *testing.T) {
	t.Parallel()

	if _, err := NewOpenAI(OpenAIConfig{BaseURL: "http://127.0.0.1:8090"}); err == nil {
		t.Fatal("NewOpenAI accepted a config without an id")
	}
	if _, err := NewOpenAI(OpenAIConfig{ID: "up-1"}); err == nil {
		t.Fatal("NewOpenAI accepted a config without a base url")
	}
}

// TestOpenAIID reports the configured identity.
func TestOpenAIID(t *testing.T) {
	t.Parallel()

	_, adapter := newTestAdapter(t, nil)
	if got := adapter.ID(); got != "up-1" {
		t.Fatalf("ID = %q, want up-1", got)
	}
}

// TestForwardPostsToCompletionPath: one POST exchange against
// <base>/v1/chat/completions with the JSON content type and the
// neutral body passed through verbatim.
func TestForwardPostsToCompletionPath(t *testing.T) {
	t.Parallel()

	var gotMethod, gotPath, gotContentType, gotBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		gotContentType = r.Header.Get("Content-Type")
		raw, _ := io.ReadAll(r.Body)
		gotBody = string(raw)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"c-1"}`))
	}))
	defer server.Close()

	adapter, err := NewOpenAI(OpenAIConfig{ID: "up-1", BaseURL: server.URL})
	if err != nil {
		t.Fatalf("NewOpenAI: %v", err)
	}
	resp, err := adapter.Forward(context.Background(), Request{Body: []byte(`{"model":"m1"}`)})
	if err != nil {
		t.Fatalf("Forward: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if gotMethod != http.MethodPost || gotPath != completionPath {
		t.Fatalf("exchange = %s %s, want POST %s", gotMethod, gotPath, completionPath)
	}
	if gotContentType != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", gotContentType)
	}
	if gotBody != `{"model":"m1"}` {
		t.Fatalf("forwarded body = %q, want the neutral body verbatim", gotBody)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if string(body) != `{"id":"c-1"}` {
		t.Fatalf("response body = %q", body)
	}
}

// TestForwardRequestHeaders: streaming sets the SSE accept header,
// an API key becomes the bearer token, and neither leaks when absent.
func TestForwardRequestHeaders(t *testing.T) {
	cases := []struct {
		name       string
		apiKey     string
		stream     bool
		wantAccept string
		wantBearer string
	}{
		{"plain buffered call", "", false, "", ""},
		{"streaming call", "", true, "text/event-stream", ""},
		{"authenticated call", "sk-up", false, "", "Bearer sk-up"},
		{"authenticated stream", "sk-up", true, "text/event-stream", "Bearer sk-up"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var gotAccept, gotAuth string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				gotAccept = r.Header.Get("Accept")
				gotAuth = r.Header.Get("Authorization")
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()

			adapter, err := NewOpenAI(OpenAIConfig{ID: "up-1", BaseURL: server.URL, APIKey: tc.apiKey})
			if err != nil {
				t.Fatalf("NewOpenAI: %v", err)
			}
			resp, err := adapter.Forward(context.Background(), Request{Stream: tc.stream})
			if err != nil {
				t.Fatalf("Forward: %v", err)
			}
			_ = resp.Body.Close()

			if gotAccept != tc.wantAccept {
				t.Fatalf("Accept = %q, want %q", gotAccept, tc.wantAccept)
			}
			if gotAuth != tc.wantBearer {
				t.Fatalf("Authorization = %q, want %q", gotAuth, tc.wantBearer)
			}
		})
	}
}

func TestForwardPropagatesRequestID(t *testing.T) {
	t.Parallel()
	var gotIDs []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotIDs = append(gotIDs, r.Header.Get("X-Request-Id"))
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	adapter, err := NewOpenAI(OpenAIConfig{ID: "up", BaseURL: server.URL})
	if err != nil {
		t.Fatalf("NewOpenAI: %v", err)
	}

	resp, err := adapter.Forward(context.Background(), Request{RequestID: "req-abc"})
	if err != nil {
		t.Fatalf("Forward: %v", err)
	}
	_ = resp.Body.Close()
	// Absent ids must not leak an empty header upstream.
	resp2, err := adapter.Forward(context.Background(), Request{})
	if err != nil {
		t.Fatalf("second Forward: %v", err)
	}
	_ = resp2.Body.Close()

	if len(gotIDs) != 2 || gotIDs[0] != "req-abc" || gotIDs[1] != "" {
		t.Fatalf("upstream X-Request-Id values = %q, want [req-abc, empty]", gotIDs)
	}
}

// TestForwardReturnsCompletedFailures: a completed 4xx/5xx exchange is
// a non-nil Response with the upstream status, headers and body — the
// port's result convention.
func TestForwardReturnsCompletedFailures(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{"error":{"message":"overloaded"}}`))
	}))
	defer server.Close()

	adapter, err := NewOpenAI(OpenAIConfig{ID: "up-1", BaseURL: server.URL})
	if err != nil {
		t.Fatalf("NewOpenAI: %v", err)
	}
	resp, err := adapter.Forward(context.Background(), Request{})
	if err != nil {
		t.Fatalf("Forward: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") != "7" {
		t.Fatalf("Retry-After = %q, want the upstream header", resp.Header.Get("Retry-After"))
	}
	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "overloaded") {
		t.Fatalf("body = %q, want the upstream error body", body)
	}
}

// TestForwardConnectionFailureIsErrorNotResponse: an exchange that
// never completes reports nil Response with an error.
func TestForwardConnectionFailureIsErrorNotResponse(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	server.Close() // nothing listens anymore

	adapter, err := NewOpenAI(OpenAIConfig{ID: "up-1", BaseURL: server.URL})
	if err != nil {
		t.Fatalf("NewOpenAI: %v", err)
	}
	resp, err := adapter.Forward(context.Background(), Request{})
	if err == nil {
		t.Fatalf("Forward = %v, nil error, want a connection failure", resp)
	}
	if resp != nil {
		t.Fatalf("Response = %v, want nil for an incomplete exchange", resp)
	}
	if !strings.Contains(err.Error(), "up-1") {
		t.Fatalf("error %q does not name the upstream", err)
	}
}

// TestForwardHonorsContextCancellation: cancellation is owned by the
// caller through ctx.
func TestForwardHonorsContextCancellation(t *testing.T) {
	t.Parallel()

	block := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-block
	}))
	defer server.Close()
	defer close(block)

	adapter, err := NewOpenAI(OpenAIConfig{ID: "up-1", BaseURL: server.URL})
	if err != nil {
		t.Fatalf("NewOpenAI: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()
	resp, err := adapter.Forward(ctx, Request{})
	if err == nil {
		t.Fatalf("Forward = %v with nil error, want cancellation", resp)
	}
	if resp != nil {
		t.Fatalf("Response = %v, want nil", resp)
	}
}

// TestForwardRejectsUnparsableBaseURL: a malformed base fails at
// request build time, inside the adapter.
func TestForwardRejectsUnparsableBaseURL(t *testing.T) {
	t.Parallel()

	adapter, err := NewOpenAI(OpenAIConfig{ID: "up-1", BaseURL: "http://exa mple.com"})
	if err != nil {
		t.Fatalf("NewOpenAI: %v", err)
	}
	resp, err := adapter.Forward(context.Background(), Request{})
	if err == nil || resp != nil {
		t.Fatalf("Forward = (%v, %v), want a build-request error", resp, err)
	}
	if !strings.Contains(err.Error(), "build request") {
		t.Fatalf("error = %q, want the build-request wrap", err)
	}
}

// TestProbeSuccess: the configured health endpoint answering 2xx
// passes.
func TestProbeSuccess(t *testing.T) {
	t.Parallel()

	var gotMethod, gotPath string
	_, adapter := newTestAdapter(t, func(server *httptest.Server, cfg *OpenAIConfig) {
		server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotMethod, gotPath = r.Method, r.URL.Path
			_, _ = w.Write([]byte("ok"))
		})
		cfg.ProbeURL = server.URL + "/healthz"
	})

	if err := adapter.Probe(context.Background()); err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if gotMethod != http.MethodGet || gotPath != "/healthz" {
		t.Fatalf("probe = %s %s, want GET /healthz", gotMethod, gotPath)
	}
}

// TestProbeRejectsUnhealthyStatus: a non-2xx health endpoint fails.
func TestProbeRejectsUnhealthyStatus(t *testing.T) {
	t.Parallel()

	_, adapter := newTestAdapter(t, func(server *httptest.Server, cfg *OpenAIConfig) {
		server.Config.Handler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
		})
		cfg.ProbeURL = server.URL + "/healthz"
	})

	if err := adapter.Probe(context.Background()); err == nil {
		t.Fatal("Probe accepted a 503 health endpoint")
	}
}

// TestProbeWithoutURL: probing is disabled by an empty ProbeURL and
// reports so instead of failing mysteriously.
func TestProbeWithoutURL(t *testing.T) {
	t.Parallel()

	_, adapter := newTestAdapter(t, nil)
	if err := adapter.Probe(context.Background()); err == nil {
		t.Fatal("Probe succeeded without a configured probe url")
	}
}

// TestProbeConnectionFailure: an unreachable health endpoint is an
// error, not a panic.
func TestProbeConnectionFailure(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	server.Close()

	adapter, err := NewOpenAI(OpenAIConfig{ID: "up-1", BaseURL: "http://127.0.0.1:1", ProbeURL: server.URL})
	if err != nil {
		t.Fatalf("NewOpenAI: %v", err)
	}
	if err := adapter.Probe(context.Background()); err == nil {
		t.Fatal("Probe succeeded against a closed server")
	}
}

// TestProbeRejectsTruncatedBody: a health endpoint that promises more
// bytes than it delivers fails the probe instead of passing.
func TestProbeRejectsTruncatedBody(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Take over the connection and answer with a Content-Length
		// larger than the bytes actually sent, then cut the connection.
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		defer func() { _ = conn.Close() }()
		_, _ = conn.Write([]byte("HTTP/1.1 200 OK\r\nContent-Length: 100\r\n\r\nshort"))
	}))
	defer server.Close()

	adapter, err := NewOpenAI(OpenAIConfig{ID: "up-1", BaseURL: server.URL, ProbeURL: server.URL + "/healthz"})
	if err != nil {
		t.Fatalf("NewOpenAI: %v", err)
	}
	if err := adapter.Probe(context.Background()); err == nil {
		t.Fatal("Probe accepted a truncated health body")
	}
}

// TestProbeRejectsUnparsableURL: a malformed probe url fails at
// request build time, inside the adapter.
func TestProbeRejectsUnparsableURL(t *testing.T) {
	t.Parallel()

	adapter, err := NewOpenAI(OpenAIConfig{ID: "up-1", BaseURL: "http://127.0.0.1:8090", ProbeURL: "http://exa mple.com/healthz"})
	if err != nil {
		t.Fatalf("NewOpenAI: %v", err)
	}
	if err := adapter.Probe(context.Background()); err == nil {
		t.Fatal("Probe accepted an unparsable probe url")
	}
}
