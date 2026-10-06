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
		return sseResponse(strings.NewReader(sseSample)), nil
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

// commitHeaderUpstream returns a stub streaming 200 over a clean
// [DONE] body whose reply header is customized by set — the fixture of
// the commit-header tests.
func commitHeaderUpstream(set func(http.Header)) *stubUpstream {
	return &stubUpstream{id: "s", fn: func(context.Context, upstream.Request) (*upstream.Response, error) {
		return stubResponse(http.StatusOK, strings.NewReader("data: [DONE]\n\n"), set), nil
	}}
}

// TestStreamCommitReplacesDocumentContentType: an upstream that labels
// its frames as a document a browser would render does not get that
// label onto the client response. The frames are SSE, so the SSE default
// is both the safe answer and the accurate one.
func TestStreamCommitReplacesDocumentContentType(t *testing.T) {
	t.Parallel()
	cand := commitHeaderUpstream(func(h http.Header) { h.Set("Content-Type", "text/html") })
	exec := New(testPolicy(), nil)

	result := execute(t, exec, []upstream.Upstream{cand}, true, "{}")
	if ct := result.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content type = %q, want the SSE default", ct)
	}
	if string(result.Body) != "data: [DONE]\n\n" {
		t.Fatalf("stream bytes altered: %q", result.Body)
	}
}

// TestStreamCommitHeadersFallBackAndPassThrough pins the commit header
// rules: a missing upstream content type becomes text/event-stream, and
// an upstream cache-control directive survives the commit.
func TestStreamCommitHeadersFallBackAndPassThrough(t *testing.T) {
	t.Parallel()
	cand := commitHeaderUpstream(func(h http.Header) { h.Set("Cache-Control", "no-cache") })
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
	cand := jsonStubUpstream(t, "s", http.StatusServiceUnavailable, `{"error":{}}`)
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
	cand := unreadableStubUpstream("s", http.StatusServiceUnavailable)
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
	cand := failingStubUpstream("s", context.DeadlineExceeded)
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
