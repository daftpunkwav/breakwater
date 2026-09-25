/**
 * @file buffered_passthrough_test
 * @description The non-streaming exchange: verbatim passthrough of the
 * buffered reply with usage extraction, retry recovery, terminal client
 * faults, last-failure passthrough, the no-candidate guard and the
 * bounded body read.
 */
package relay

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/daftpunkwav/breakwater/internal/retry"
	"github.com/daftpunkwav/breakwater/internal/upstream"
)

func TestBufferedPassthroughWithUsage(t *testing.T) {
	t.Parallel()
	body := `{"choices":[],"usage":{"prompt_tokens":11,"completion_tokens":7,"total_tokens":18}}`
	cand := &stubUpstream{id: "a", fn: func(context.Context, upstream.Request) (*upstream.Response, error) {
		return jsonResponse(t, http.StatusOK, body), nil
	}}
	exec := New(testPolicy(), nil)

	result := execute(t, exec, []upstream.Upstream{cand}, false, "{}")
	if result.Status != http.StatusOK {
		t.Fatalf("status = %d, want 200", result.Status)
	}
	if result.UpstreamID != "a" {
		t.Fatalf("served by %q, want a", result.UpstreamID)
	}
	if !result.UsageKnown || result.Usage.TotalTokens != 18 {
		t.Fatalf("usage not extracted: %+v known=%v", result.Usage, result.UsageKnown)
	}
	if string(result.Body) != body {
		t.Fatalf("body mismatch: %q", result.Body)
	}
	if ct := result.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content type = %q", ct)
	}
}

func TestRetryRecoversFromServerError(t *testing.T) {
	t.Parallel()
	calls := 0
	cand := &stubUpstream{id: "a", fn: func(context.Context, upstream.Request) (*upstream.Response, error) {
		calls++
		if calls == 1 {
			return jsonResponse(t, http.StatusBadGateway, `{"error":{}}`), nil
		}
		return jsonResponse(t, http.StatusOK, `{"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`), nil
	}}
	exec := New(testPolicy(), nil)

	result := execute(t, exec, []upstream.Upstream{cand}, false, "{}")
	if result.Status != http.StatusOK || result.Attempts != 2 {
		t.Fatalf("status = %d attempts = %d, want 200/2", result.Status, result.Attempts)
	}
}

func TestClientErrorIsTerminalPassthrough(t *testing.T) {
	t.Parallel()
	calls := 0
	cand := &stubUpstream{id: "a", fn: func(context.Context, upstream.Request) (*upstream.Response, error) {
		calls++
		return jsonResponse(t, http.StatusUnauthorized, `{"error":{"code":"invalid_key"}}`), nil
	}}
	exec := New(testPolicy(), nil)

	result := execute(t, exec, []upstream.Upstream{cand}, false, "{}")
	if result.Status != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 passthrough", result.Status)
	}
	if calls != 1 {
		t.Fatalf("attempts = %d, want 1: client faults never retry", calls)
	}
	if string(result.Body) != `{"error":{"code":"invalid_key"}}` {
		t.Fatalf("body mismatch: %q", result.Body)
	}
}

func TestAllCandidatesFailingPassesThroughLastStatus(t *testing.T) {
	t.Parallel()
	cand := &stubUpstream{id: "a", fn: func(_ context.Context, _ upstream.Request) (*upstream.Response, error) {
		return jsonResponse(t, http.StatusTooManyRequests, `{"error":{}}`), nil
	}}
	exec := New(testPolicy(), nil)

	result := execute(t, exec, []upstream.Upstream{cand}, false, "{}")
	if result.Status != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429 passthrough", result.Status)
	}
	if result.UpstreamID != "a" {
		t.Fatalf("served by %q, want a: error passthroughs keep their upstream", result.UpstreamID)
	}
}

func TestNoCandidatesRendersGatewayError(t *testing.T) {
	t.Parallel()
	exec := New(testPolicy(), nil)
	rec := httptest.NewRecorder()
	result := exec.Execute(context.Background(), Job{Model: "m", Out: rec})
	if result.Status != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", result.Status)
	}
	if !strings.Contains(rec.Body.String(), `"code":"no_upstream"`) {
		t.Fatalf("body = %q, want gateway envelope", rec.Body.String())
	}
}

// TestBufferedBodyReadFailureFailsClosed pins the read-failure branch of
// the buffered exchange: an unreadable upstream body is a server fault
// that renders as the gateway envelope once the attempt cap is reached.
func TestBufferedBodyReadFailureFailsClosed(t *testing.T) {
	t.Parallel()
	cand := &stubUpstream{id: "a", fn: func(context.Context, upstream.Request) (*upstream.Response, error) {
		header := http.Header{}
		header.Set("Content-Type", "application/json")
		return &upstream.Response{
			StatusCode: http.StatusOK,
			Header:     header,
			Body:       io.NopCloser(errReader{}),
		}, nil
	}}
	exec := New(retry.Policy{MaxAttempts: 1}, nil)

	result := execute(t, exec, []upstream.Upstream{cand}, false, "{}")
	if result.Status != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502 upstream_unreachable", result.Status)
	}
	if !strings.Contains(string(result.Body), `"code":"upstream_unreachable"`) {
		t.Fatalf("body = %q, want gateway envelope", result.Body)
	}
}

// errReader fails every read, simulating a connection reset under the
// buffered exchange.
type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("connection reset by peer") }

// TestReadBoundedRejectsOversizedBody pins the fail-closed body cap: a
// body past maxResponseBytes is refused, never truncated into a lie.
func TestReadBoundedRejectsOversizedBody(t *testing.T) {
	t.Parallel()
	oversized := strings.Repeat("x", maxResponseBytes+1)
	_, err := readBounded(strings.NewReader(oversized), maxResponseBytes)
	if err == nil {
		t.Fatal("oversized body accepted")
	}
	if !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("err = %v, want the size-limit message", err)
	}
}

// TestReadBoundedPropagatesReadFailure pins that transport-level read
// errors surface instead of masquerading as an empty body.
func TestReadBoundedPropagatesReadFailure(t *testing.T) {
	t.Parallel()
	_, err := readBounded(errReader{}, maxResponseBytes)
	if err == nil {
		t.Fatal("read failure swallowed")
	}
}
