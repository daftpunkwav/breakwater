/**
 * @file executor_test
 * @description Relay engine tests: passthrough fidelity, failover and
 * retry under the budgets, and the honest termination of broken
 * streams (invariants I5/I6).
 */
package relay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/daftpunkwav/breakwater/internal/protocol"
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
	if result.UpstreamID != "s" {
		t.Fatalf("aborted stream served by %q, want s (the committing candidate)", result.UpstreamID)
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

// dripStream returns a body reader that emits n data frames with a
// delay between them, then a clean [DONE]. Like a real HTTP response
// body, it fails with the context's error when the context is
// cancelled mid-drip.
func dripStream(ctx context.Context, n int, delay time.Duration) io.Reader {
	pr, pw := io.Pipe()
	go func() {
		for i := 1; i <= n; i++ {
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-ctx.Done():
				timer.Stop()
				pw.CloseWithError(ctx.Err())
				return
			}
			timer.Stop()
			if _, err := fmt.Fprintf(pw, "data: {\"choices\":[{\"delta\":{\"content\":\"c%d\"}}]}\n\n", i); err != nil {
				pw.CloseWithError(err)
				return
			}
		}
		_ = pw.Close()
	}()
	return pr
}

// TestStreamOutlivesAttemptTimeout pins the streaming time budget: the
// attempt timeout bounds time-to-first-byte only — a body that keeps
// streaming past it must survive.
func TestStreamOutlivesAttemptTimeout(t *testing.T) {
	t.Parallel()
	cand := &stubUpstream{id: "s", fn: func(ctx context.Context, _ upstream.Request) (*upstream.Response, error) {
		header := http.Header{}
		header.Set("Content-Type", "text/event-stream")
		return &upstream.Response{
			StatusCode: http.StatusOK,
			Header:     header,
			Body:       io.NopCloser(dripStream(ctx, 5, 60*time.Millisecond)), // ~300ms total
		}, nil
	}}
	exec := New(retry.Policy{MaxAttempts: 1, AttemptTimeout: 80 * time.Millisecond}, nil)

	result := execute(t, exec, []upstream.Upstream{cand}, true, "{}")
	if result.Aborted {
		t.Fatalf("stream aborted: the attempt timeout must not bound the body")
	}
	if !strings.Contains(string(result.Body), "c5") {
		t.Fatalf("late chunks lost: %q", result.Body)
	}
}

// TestStreamTimeToFirstByteRetries pins that a dead-slow header phase
// still counts as a retryable timeout (failover territory).
func TestStreamTimeToFirstByteRetries(t *testing.T) {
	t.Parallel()
	calls := 0
	cand := &stubUpstream{id: "s", fn: func(ctx context.Context, _ upstream.Request) (*upstream.Response, error) {
		calls++
		if calls == 1 {
			select {
			case <-time.After(400 * time.Millisecond):
				t.Error("first attempt should have been cut by the ttft timer")
				return nil, errors.New("unreachable")
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		header := http.Header{}
		header.Set("Content-Type", "text/event-stream")
		return &upstream.Response{
			StatusCode: http.StatusOK,
			Header:     header,
			Body:       io.NopCloser(strings.NewReader("data: [DONE]\n\n")),
		}, nil
	}}
	exec := New(retry.Policy{MaxAttempts: 2, AttemptTimeout: 60 * time.Millisecond}, nil)

	result := execute(t, exec, []upstream.Upstream{cand}, true, "{}")
	if result.Status != http.StatusOK || result.Attempts != 2 {
		t.Fatalf("status = %d attempts = %d, want 200/2 (ttft timeout must retry)", result.Status, result.Attempts)
	}
}

// TestStreamCeilingTerminatesHonestly pins the stream ceiling: a body
// that runs past it ends through the error contract as upstream_timeout.
func TestStreamCeilingTerminatesHonestly(t *testing.T) {
	t.Parallel()
	cand := &stubUpstream{id: "s", fn: func(ctx context.Context, _ upstream.Request) (*upstream.Response, error) {
		header := http.Header{}
		header.Set("Content-Type", "text/event-stream")
		return &upstream.Response{
			StatusCode: http.StatusOK,
			Header:     header,
			Body:       io.NopCloser(dripStream(ctx, 20, 50*time.Millisecond)),
		}, nil
	}}
	exec := New(retry.Policy{MaxAttempts: 1, AttemptTimeout: 2 * time.Second},
		nil, WithStreamTimeout(100*time.Millisecond))

	result := execute(t, exec, []upstream.Upstream{cand}, true, "{}")
	if !result.Aborted {
		t.Fatal("stream past its ceiling must abort")
	}
	if !strings.Contains(string(result.Body), `"code":"upstream_timeout"`) {
		t.Fatalf("abort code = %q", result.Body)
	}
	// The canonical wire's frozen termination: one error event, then [DONE].
	if !strings.HasSuffix(string(result.Body), "data: [DONE]\n\n") {
		t.Fatalf("aborted stream must end with the [DONE] sentinel: %q", result.Body)
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

// TestStreamTTFTExpiryDuringForwardRetries pins the race where the TTFT
// timer expires while Forward is still in flight: the headers land into
// an already-cancelled context, and the attempt must fail as the
// retryable timeout it is — never commit a stream that cannot pump.
func TestStreamTTFTExpiryDuringForwardRetries(t *testing.T) {
	t.Parallel()
	calls := 0
	sseHeader := func() http.Header {
		header := http.Header{}
		header.Set("Content-Type", "text/event-stream")
		return header
	}
	cand := &stubUpstream{id: "s", fn: func(context.Context, upstream.Request) (*upstream.Response, error) {
		calls++
		if calls == 1 {
			// Outlives the TTFT budget; the timer fires mid-call.
			time.Sleep(80 * time.Millisecond)
		}
		return &upstream.Response{
			StatusCode: http.StatusOK,
			Header:     sseHeader(),
			Body:       io.NopCloser(strings.NewReader("data: [DONE]\n\n")),
		}, nil
	}}
	exec := New(retry.Policy{MaxAttempts: 2, AttemptTimeout: 30 * time.Millisecond}, nil)

	result := execute(t, exec, []upstream.Upstream{cand}, true, "{}")
	if result.Aborted {
		t.Fatal("aborted: a TTFT expiry must fail the attempt before commit, not abort a committed stream")
	}
	if result.Status != http.StatusOK || result.Attempts != 2 {
		t.Fatalf("status = %d attempts = %d, want 200/2 (retryable TTFT timeout)", result.Status, result.Attempts)
	}
}

// TestStreamAbortReusesTheTranscoderInstance locks the translated-wire
// abort contract: the failure frame must carry the same object id the
// preamble introduced — a fresh transcoder would invent a new one.
func TestStreamAbortReusesTheTranscoderInstance(t *testing.T) {
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
	rec := httptest.NewRecorder()
	exec := New(testPolicy(), nil)
	result := exec.Execute(context.Background(), Job{
		Model:      "test-model",
		Stream:     true,
		Candidates: []upstream.Upstream{cand},
		Wire:       protocol.WireFor(protocol.FormatOpenAIResponses),
		Out:        rec,
	})
	if !result.Aborted {
		t.Fatal("result not marked aborted")
	}
	body := rec.Body.String()
	startID := extractJSONField(t, body, "response.created", "id")
	failID := extractJSONField(t, body, "response.failed", "id")
	if startID == "" || failID == "" {
		t.Fatalf("missing ids: created=%q failed=%q body=%s", startID, failID, body)
	}
	if startID != failID {
		t.Fatalf("abort frame id %q does not match the stream preamble id %q", failID, startID)
	}
}

// extractJSONField pulls one field from the payload of a named SSE
// event in a recorded stream, looking one object level deep.
func extractJSONField(t *testing.T, stream, event, field string) string {
	t.Helper()
	for _, frame := range strings.Split(stream, "\n\n") {
		lines := strings.Split(frame, "\n")
		if len(lines) < 2 || lines[0] != "event: "+event {
			continue
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(strings.TrimPrefix(lines[1], "data: ")), &payload); err != nil {
			t.Fatalf("event %s payload: %v", event, err)
		}
		if id, ok := payload[field].(string); ok {
			return id
		}
		for _, nested := range payload {
			if obj, ok := nested.(map[string]any); ok {
				if id, ok := obj[field].(string); ok {
					return id
				}
			}
		}
	}
	return ""
}
