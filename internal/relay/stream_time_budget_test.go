/**
 * @file stream_time_budget_test
 * @description The streaming time budget: the attempt timeout bounds
 * time-to-first-byte only, a body that outlives it must survive, and
 * the optional ceiling terminates an endless stream honestly.
 */
package relay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/daftpunkwav/breakwater/internal/retry"
	"github.com/daftpunkwav/breakwater/internal/upstream"
)

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
