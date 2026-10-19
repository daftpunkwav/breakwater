/**
 * @file slowcall_test
 * @description The slow-call classification at the attempt layer: a
 * healthy attempt whose responsiveness passed the threshold reports
 * OutcomeSlow — buffered by its full duration, streamed by the time to
 * first byte — and without a threshold every healthy attempt reports
 * fast however long it took.
 */
package relay

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/daftpunkwav/breakwater/internal/circuit"
	"github.com/daftpunkwav/breakwater/internal/upstream"
)

// outcomeRecorder grants everything and captures every reported
// outcome.
type outcomeRecorder struct {
	mu       sync.Mutex
	outcomes []circuit.Outcome
}

func (b *outcomeRecorder) Allow(_ context.Context, _ string) (circuit.Permission, bool) {
	return b, true
}

func (b *outcomeRecorder) Report(outcome circuit.Outcome) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.outcomes = append(b.outcomes, outcome)
}

func (b *outcomeRecorder) StateOf(context.Context, string) circuit.State {
	return circuit.StateClosed
}

func (b *outcomeRecorder) Reset(context.Context, string) {}

func (b *outcomeRecorder) last() circuit.Outcome {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.outcomes) == 0 {
		return -1
	}
	return b.outcomes[len(b.outcomes)-1]
}

// attemptClock is a manually advanced clock. The slow-call
// classification compares a measured duration against a threshold, and
// a wall-clock sleep races that comparison: the attempt's whole
// exchange path counts against the threshold, so a loaded runner
// stretches a 5ms attempt past a 20ms one. The test drives the clock
// instead of waiting for it, and the measured duration is exactly what
// the test advanced.
type attemptClock struct {
	mu  sync.Mutex
	now time.Time
}

func newAttemptClock() *attemptClock {
	return &attemptClock{now: time.Now()}
}

func (c *attemptClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

// advance moves the clock forward by d.
func (c *attemptClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// bufferedAttemptOutcome drives one buffered attempt through an
// executor over an upstream that answers after delay; opts customize
// the executor (typically WithSlowCallThreshold, whose omission
// exercises the zero default). The returned outcome is what the breaker
// received. The attempt runs on the test's clock, so delay is the
// measured duration of the attempt, not a real wait.
func bufferedAttemptOutcome(t *testing.T, delay time.Duration, opts ...Option) circuit.Outcome {
	t.Helper()
	br := &outcomeRecorder{}
	clock := newAttemptClock()
	up := &stubUpstream{id: "slow", fn: func(_ context.Context, _ upstream.Request) (*upstream.Response, error) {
		clock.advance(delay)
		return jsonResponse(t, http.StatusOK, `{"choices":[{"message":{"content":"ok"}}]}`), nil
	}}
	exec := New(testPolicy(), nil, append([]Option{WithBreaker(br), WithClock(clock.Now)}, opts...)...)

	execute(t, exec, []upstream.Upstream{up}, false, `{"model":"m","messages":[]}`)
	return br.last()
}

// TestSlowBufferedAttemptReportsSlow: a healthy buffered reply past the
// threshold reports OutcomeSlow to the breaker.
func TestSlowBufferedAttemptReportsSlow(t *testing.T) {
	t.Parallel()
	if got := bufferedAttemptOutcome(t, 80*time.Millisecond, WithSlowCallThreshold(20*time.Millisecond)); got != circuit.OutcomeSlow {
		t.Fatalf("reported outcome = %v, want slow", got)
	}
}

// TestFastBufferedAttemptStaysSuccess: under the threshold the attempt
// reports as success.
func TestFastBufferedAttemptStaysSuccess(t *testing.T) {
	t.Parallel()
	if got := bufferedAttemptOutcome(t, 5*time.Millisecond, WithSlowCallThreshold(20*time.Millisecond)); got != circuit.OutcomeSuccess {
		t.Fatalf("reported outcome = %v, want success", got)
	}
}

// TestSlowThresholdIsExclusive: the threshold is the boundary the
// classification compares against, and a duration exactly on it is not
// slow — only a duration past it is. The comparison is what the
// slow-call strategy counts on, so its edge is pinned here rather than
// left to the wall clock.
func TestSlowThresholdIsExclusive(t *testing.T) {
	t.Parallel()
	if got := bufferedAttemptOutcome(t, 20*time.Millisecond, WithSlowCallThreshold(20*time.Millisecond)); got != circuit.OutcomeSuccess {
		t.Fatalf("reported outcome = %v, want success on the threshold", got)
	}
	if got := bufferedAttemptOutcome(t, 20*time.Millisecond+time.Nanosecond, WithSlowCallThreshold(20*time.Millisecond)); got != circuit.OutcomeSlow {
		t.Fatalf("reported outcome = %v, want slow past the threshold", got)
	}
}

// TestNoThresholdNeverReclassifies: without a threshold a healthy
// attempt reports success however long it took — the classification is
// opt-in.
func TestNoThresholdNeverReclassifies(t *testing.T) {
	t.Parallel()
	if got := bufferedAttemptOutcome(t, 80*time.Millisecond); got != circuit.OutcomeSuccess {
		t.Fatalf("reported outcome = %v, want success without a threshold", got)
	}
}

// TestSlowStreamTTFTReportsSlow: for a stream the responsiveness is the
// time to first byte — a late header reports slow even though the body
// itself is tiny.
func TestSlowStreamTTFTReportsSlow(t *testing.T) {
	t.Parallel()
	br := &outcomeRecorder{}
	clock := newAttemptClock()
	sse := "data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\ndata: [DONE]\n\n"
	slow := &stubUpstream{id: "slow", fn: func(_ context.Context, _ upstream.Request) (*upstream.Response, error) {
		clock.advance(80 * time.Millisecond)
		resp := jsonResponse(t, http.StatusOK, sse)
		resp.Header.Set("Content-Type", "text/event-stream")
		return resp, nil
	}}
	exec := New(testPolicy(), nil, WithBreaker(br), WithClock(clock.Now), WithSlowCallThreshold(20*time.Millisecond))

	execute(t, exec, []upstream.Upstream{slow}, true, `{"model":"m","stream":true,"messages":[]}`)
	if got := br.last(); got != circuit.OutcomeSlow {
		t.Fatalf("reported outcome = %v, want slow from the TTFT", got)
	}
}
