/**
 * @file stream_passthrough_test
 * @description The committed streaming passthrough before any failure:
 * byte fidelity, usage scraping, commit headers, the pre-first-byte HTTP
 * status path for upstream errors, and the error-body read that must
 * happen while the stream lease is alive.
 */
package relay

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/daftpunkwav/breakwater/internal/retry"
	"github.com/daftpunkwav/breakwater/internal/upstream"
)

const sseSample = "data: {\"choices\":[{\"delta\":{\"content\":\"he\"}}]}\n\n" +
	"data: {\"choices\":[{\"delta\":{\"content\":\"llo\"}}]}\n\n" +
	"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":2,\"total_tokens\":5}}\n\n" +
	"data: [DONE]\n\n"

func TestStreamPassthroughPreservesBytesAndScrapesUsage(t *testing.T) {
	t.Parallel()
	cand := &stubUpstream{id: "s", fn: func(_ context.Context, req upstream.Request) (*upstream.Response, error) {
		if !req.Stream {
			t.Error("stream job forwarded with stream=false")
		}
		header := http.Header{}
		header.Set("Content-Type", "text/event-stream")
		return &upstream.Response{
			StatusCode: http.StatusOK,
			Header:     header,
			Body:       io.NopCloser(strings.NewReader(sseSample)),
		}, nil
	}}
	exec := New(testPolicy(), nil)

	result := execute(t, exec, []upstream.Upstream{cand}, true, "{}")
	if !result.Streamed || result.Aborted {
		t.Fatalf("streamed=%v aborted=%v, want true/false", result.Streamed, result.Aborted)
	}
	if !result.UsageKnown || result.Usage.TotalTokens != 5 {
		t.Fatalf("stream usage not scraped: %+v known=%v", result.Usage, result.UsageKnown)
	}
	if string(result.Body) != sseSample {
		t.Fatalf("stream bytes altered:\n got %q\nwant %q", result.Body, sseSample)
	}
	if ct := result.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content type = %q", ct)
	}
}

// TestStreamCommitHeadersFallBackAndPassThrough pins the commit header
// rules: a missing upstream content type becomes text/event-stream, and
// an upstream cache-control directive survives the commit.
func TestStreamCommitHeadersFallBackAndPassThrough(t *testing.T) {
	t.Parallel()
	cand := &stubUpstream{id: "s", fn: func(context.Context, upstream.Request) (*upstream.Response, error) {
		header := http.Header{}
		header.Set("Cache-Control", "no-cache")
		return &upstream.Response{
			StatusCode: http.StatusOK,
			Header:     header,
			Body:       io.NopCloser(strings.NewReader("data: [DONE]\n\n")),
		}, nil
	}}
	exec := New(testPolicy(), nil)

	result := execute(t, exec, []upstream.Upstream{cand}, true, "{}")
	if result.Aborted {
		t.Fatal("clean stream marked aborted")
	}
	if ct := result.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content type = %q, want the text/event-stream fallback", ct)
	}
	if cc := result.Header.Get("Cache-Control"); cc != "no-cache" {
		t.Fatalf("cache control = %q, want passthrough", cc)
	}
}

func TestStreamServerErrorBeforeFirstByteStaysHTTP(t *testing.T) {
	t.Parallel()
	cand := &stubUpstream{id: "s", fn: func(context.Context, upstream.Request) (*upstream.Response, error) {
		return jsonResponse(t, http.StatusServiceUnavailable, `{"error":{}}`), nil
	}}
	exec := New(testPolicy(), nil)

	result := execute(t, exec, []upstream.Upstream{cand}, true, "{}")
	if result.Status != http.StatusServiceUnavailable || result.Streamed {
		t.Fatalf("status = %d streamed = %v, want 503/false: pre-first-byte failures use HTTP status", result.Status, result.Streamed)
	}
}

// TestStreamErrorBodyReadFailureFailsAttempt pins the read-failure guard
// of the pre-commit error path: an unreadable error body fails the
// attempt instead of committing a broken stream.
func TestStreamErrorBodyReadFailureFailsAttempt(t *testing.T) {
	t.Parallel()
	cand := &stubUpstream{id: "s", fn: func(context.Context, upstream.Request) (*upstream.Response, error) {
		header := http.Header{}
		header.Set("Content-Type", "application/json")
		return &upstream.Response{
			StatusCode: http.StatusServiceUnavailable,
			Header:     header,
			Body:       io.NopCloser(errReader{}),
		}, nil
	}}
	exec := New(retry.Policy{MaxAttempts: 1}, nil)

	result := execute(t, exec, []upstream.Upstream{cand}, true, "{}")
	if result.Streamed {
		t.Fatal("a stream whose error body could not be read must not commit")
	}
	if result.Status != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 upstream_unreachable", result.Status)
	}
}

// TestStreamForwardErrorFailsAttempt pins that a stream attempt whose
// Forward never answers (connection refused) fails without committing.
func TestStreamForwardErrorFailsAttempt(t *testing.T) {
	t.Parallel()
	cand := &stubUpstream{id: "s", fn: func(context.Context, upstream.Request) (*upstream.Response, error) {
		return nil, context.DeadlineExceeded
	}}
	exec := New(retry.Policy{MaxAttempts: 1, AttemptTimeout: time.Second}, nil)

	result := execute(t, exec, []upstream.Upstream{cand}, true, "{}")
	if result.Streamed {
		t.Fatal("nothing may commit when Forward never answered")
	}
	if result.Status != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 upstream_unreachable", result.Status)
	}
}

// TestStreamErrorPassthroughOverRealTransport pins the error-body read
// against a real HTTP upstream: the response body dies with the forward
// context, so the exchange must read it while the lease is still alive —
// otherwise the upstream 429 degrades into a gateway envelope.
func TestStreamErrorPassthroughOverRealTransport(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusTooManyRequests)
		// Flush so Forward returns on the headers alone; the error body
		// then trickles in later. A read issued after the forward
		// context was cancelled fails visibly instead of hitting the
		// transport's already-buffered bytes.
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		time.Sleep(100 * time.Millisecond)
		_, _ = w.Write([]byte(`{"error":{"message":"slow down"}}`))
	}))
	t.Cleanup(srv.Close)
	cand, err := upstream.NewOpenAI(upstream.OpenAIConfig{ID: "real", BaseURL: srv.URL})
	if err != nil {
		t.Fatalf("build adapter: %v", err)
	}
	exec := New(testPolicy(), nil)

	result := execute(t, exec, []upstream.Upstream{cand}, true, "{}")
	if result.Status != http.StatusTooManyRequests || result.UpstreamID != "real" {
		t.Fatalf("status = %d upstream = %q body = %q, want the upstream 429 passthrough",
			result.Status, result.UpstreamID, result.Body)
	}
	if !strings.Contains(string(result.Body), "slow down") {
		t.Fatalf("body = %q, want the upstream error body verbatim", result.Body)
	}
}
