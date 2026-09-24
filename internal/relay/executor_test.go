/**
 * @file executor_test
 * @description Relay engine tests: passthrough fidelity, failover and
 * retry under the budgets, and the honest termination of broken
 * streams (invariants I5/I6).
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

// stubUpstream answers Forward from a function.
type stubUpstream struct {
	id string
	fn func(ctx context.Context, req upstream.Request) (*upstream.Response, error)
}

func (s *stubUpstream) ID() string { return s.id }

func (s *stubUpstream) Forward(ctx context.Context, req upstream.Request) (*upstream.Response, error) {
	return s.fn(ctx, req)
}

func (s *stubUpstream) Probe(context.Context) error { return nil }

func jsonResponse(t *testing.T, status int, body string) *upstream.Response {
	t.Helper()
	header := http.Header{}
	header.Set("Content-Type", "application/json")
	return &upstream.Response{
		StatusCode: status,
		Header:     header,
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func testPolicy() retry.Policy {
	return retry.Policy{MaxAttempts: 3}
}

// capturedResult carries the Result plus what reached the client.
type capturedResult struct {
	Result
	Body   []byte
	Header http.Header
}

func execute(t *testing.T, exec *Executor, candidates []upstream.Upstream, stream bool, body string) capturedResult {
	t.Helper()
	rec := httptest.NewRecorder()
	result := exec.Execute(context.Background(), Job{
		Model:      "test-model",
		Stream:     stream,
		Body:       []byte(body),
		Candidates: candidates,
		Out:        rec,
	})
	return capturedResult{Result: result, Body: rec.Body.Bytes(), Header: rec.Header()}
}

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

func TestFailoverWalksCandidates(t *testing.T) {
	t.Parallel()
	primary := &stubUpstream{id: "primary", fn: func(context.Context, upstream.Request) (*upstream.Response, error) {
		return jsonResponse(t, http.StatusServiceUnavailable, `{}`), nil
	}}
	fallback := &stubUpstream{id: "fallback", fn: func(context.Context, upstream.Request) (*upstream.Response, error) {
		return jsonResponse(t, http.StatusOK, `{"usage":{"total_tokens":5}}`), nil
	}}
	exec := New(testPolicy(), nil)

	result := execute(t, exec, []upstream.Upstream{primary, fallback}, false, "{}")
	if result.Status != http.StatusOK || result.UpstreamID != "fallback" {
		t.Fatalf("status = %d served by %q, want 200/fallback", result.Status, result.UpstreamID)
	}
	if result.Attempts != 2 || result.Retries != 1 {
		t.Fatalf("attempts = %d retries = %d, want 2/1", result.Attempts, result.Retries)
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

func TestRetryBudgetExhaustionRendersGatewayError(t *testing.T) {
	t.Parallel()
	calls := 0
	cand := &stubUpstream{id: "a", fn: func(context.Context, upstream.Request) (*upstream.Response, error) {
		calls++
		return jsonResponse(t, http.StatusInternalServerError, `{}`), nil
	}}
	exec := New(testPolicy(), retry.NewBudget(0))

	result := execute(t, exec, []upstream.Upstream{cand}, false, "{}")
	if result.Status != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", result.Status)
	}
	if calls != 1 {
		t.Fatalf("attempts = %d, want 1", calls)
	}
	if !strings.Contains(string(result.Body), `"code":"budget_exhausted"`) {
		t.Fatalf("body = %q, want budget envelope", result.Body)
	}
}

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

// brokenReader yields some bytes then fails, simulating an upstream
// connection reset mid-stream.
type brokenReader struct {
	data string
	off  int
}

func (b *brokenReader) Read(p []byte) (int, error) {
	if b.off >= len(b.data) {
		return 0, errors.New("connection reset by peer")
	}
	n := copy(p, b.data[b.off:])
	b.off += n
	return n, nil
}

func TestStreamAbortTerminatesHonestly(t *testing.T) {
	t.Parallel()
	partial := "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n"
	cand := &stubUpstream{id: "s", fn: func(context.Context, upstream.Request) (*upstream.Response, error) {
		header := http.Header{}
		header.Set("Content-Type", "text/event-stream")
		return &upstream.Response{
			StatusCode: http.StatusOK,
			Header:     header,
			Body:       io.NopCloser(&brokenReader{data: partial}),
		}, nil
	}}
	exec := New(testPolicy(), nil)

	result := execute(t, exec, []upstream.Upstream{cand}, true, "{}")
	if !result.Aborted {
		t.Fatal("result not marked aborted")
	}
	body := string(result.Body)
	if !strings.HasPrefix(body, partial) {
		t.Fatalf("delivered bytes lost: %q", body)
	}
	wantTail := "event: error\n" +
		`data: {"error":{"message":"upstream stream failed mid-flight: connection reset by peer","type":"gateway_error","code":"upstream_reset"}}` +
		"\n\ndata: [DONE]\n\n"
	if !strings.HasSuffix(body, wantTail) {
		t.Fatalf("abort contract violated, tail = %q", body[len(body)-200:])
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
