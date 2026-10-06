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
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/daftpunkwav/breakwater/internal/circuit"
	"github.com/daftpunkwav/breakwater/internal/retry"
	"github.com/daftpunkwav/breakwater/internal/upstream"
)

// TestStreamOutlivesAttemptTimeout pins the streaming time budget: the
// attempt timeout bounds time-to-first-byte only — a body that keeps
// streaming past it must survive.
func TestStreamOutlivesAttemptTimeout(t *testing.T) {
	t.Parallel()
	cand := sseDripUpstream(5, 60*time.Millisecond) // ~300ms total
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
			case <-time.After(5 * time.Second):
				t.Error("first attempt should have been cut by the ttft timer")
				return nil, errors.New("unreachable")
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return sseResponse(strings.NewReader("data: [DONE]\n\n")), nil
	}}
	// The margins are deliberately wide: the ttft timer must win its
	// race against the parked first attempt by seconds, and the second
	// attempt must land its headers seconds before its own ttft budget
	// expires, so goroutine-scheduling jitter on a loaded machine cannot
	// flip either verdict.
	exec := New(retry.Policy{MaxAttempts: 2, AttemptTimeout: 2 * time.Second}, nil)

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
	cand := &stubUpstream{id: "s", fn: func(context.Context, upstream.Request) (*upstream.Response, error) {
		calls++
		if calls == 1 {
			// Outlives the TTFT budget by a wide margin: the timer must
			// win the race even on a heavily loaded machine, where
			// goroutine scheduling can delay the AfterFunc by tens of
			// milliseconds.
			time.Sleep(250 * time.Millisecond)
		}
		return sseResponse(strings.NewReader("data: [DONE]\n\n")), nil
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
	cand := sseDripUpstream(20, 50*time.Millisecond)
	exec := New(retry.Policy{MaxAttempts: 1, AttemptTimeout: 2 * time.Second},
		nil, WithStreamTimeout(100*time.Millisecond))

	result := execute(t, exec, []upstream.Upstream{cand}, true, "{}")
	if !result.Aborted {
		t.Fatal("stream past its ceiling must abort")
	}
	if !strings.Contains(string(result.Body), `"code":"upstream_timeout"`) {
		t.Fatalf("abort code = %q", result.Body)
	}
	// The canonical wire's termination: one error event, then [DONE].
	if !strings.HasSuffix(string(result.Body), "data: [DONE]\n\n") {
		t.Fatalf("aborted stream must end with the [DONE] sentinel: %q", result.Body)
	}
}

// TestStreamCeilingNeverPunishesTheBreaker pins the attribution: the
// gateway cutting a stream under its own ceiling is policy execution,
// not upstream evidence — a threshold-of-one breaker stays closed
// across repeated ceiling cuts, where a single server fault would
// open it.
func TestStreamCeilingNeverPunishesTheBreaker(t *testing.T) {
	t.Parallel()
	cand := sseDripUpstream(20, 50*time.Millisecond)
	breaker := circuit.NewRegistry(circuit.Config{FailThreshold: 1, Cooldown: time.Hour, ProbeTimeout: time.Second})
	exec := New(retry.Policy{MaxAttempts: 1, AttemptTimeout: 2 * time.Second},
		nil, WithStreamTimeout(100*time.Millisecond), WithBreaker(breaker))

	for range 3 {
		result := execute(t, exec, []upstream.Upstream{cand}, true, "{}")
		if !result.Aborted {
			t.Fatal("stream past its ceiling must abort")
		}
	}
	if got := breaker.StateOf(context.Background(), "s"); got != circuit.StateClosed {
		t.Fatalf("breaker state = %s after 3 ceiling-cut streams, want closed: the ceiling is gateway policy, not upstream evidence", got)
	}
}

// TestStreamCeilingBeforeHeadersFailsOver: with the ceiling tighter than
// the attempt timeout, an upstream that never answers is cut by the
// gateway's own policy. That must read as a timeout the loop may fail
// over on, and must leave the breaker no evidence of upstream fault —
// charging the upstream for the gateway's configuration would eject
// healthy candidates.
func TestStreamCeilingBeforeHeadersFailsOver(t *testing.T) {
	t.Parallel()
	blocked := &stubUpstream{id: "slow", fn: func(ctx context.Context, _ upstream.Request) (*upstream.Response, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	healthy := &stubUpstream{id: "healthy", fn: func(_ context.Context, _ upstream.Request) (*upstream.Response, error) {
		return sseResponse(strings.NewReader("data: [DONE]\n\n")), nil
	}}
	breaker := circuit.NewRegistry(circuit.Config{FailThreshold: 2, Cooldown: time.Minute, ProbeTimeout: time.Second})
	exec := New(retry.Policy{MaxAttempts: 2, AttemptTimeout: 2 * time.Second}, nil,
		WithStreamTimeout(50*time.Millisecond), WithBreaker(breaker))

	result := execute(t, exec, []upstream.Upstream{blocked, healthy}, true, "{}")
	if result.Status != http.StatusOK {
		t.Fatalf("status = %d body = %s, want the healthy candidate to answer", result.Status, result.Body)
	}
	if result.UpstreamID != "healthy" {
		t.Fatalf("served by %q, want failover to the healthy candidate", result.UpstreamID)
	}
	if got := breaker.StateOf(context.Background(), "slow"); got == circuit.StateOpen {
		t.Fatal("the gateway's own ceiling opened the breaker on an upstream that never even answered")
	}
}
