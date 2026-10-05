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

// bufferedAttemptOutcome drives one buffered attempt through an
// executor over an upstream that sleeps delay before answering an OK;
// opts customize the executor (typically WithSlowCallThreshold, whose
// omission exercises the zero default). The returned outcome is what
// the breaker received.
func bufferedAttemptOutcome(t *testing.T, delay time.Duration, opts ...Option) circuit.Outcome {
	t.Helper()
	br := &outcomeRecorder{}
	up := &stubUpstream{id: "slow", fn: func(_ context.Context, _ upstream.Request) (*upstream.Response, error) {
		time.Sleep(delay)
		return jsonResponse(t, http.StatusOK, `{"choices":[{"message":{"content":"ok"}}]}`), nil
	}}
	exec := New(testPolicy(), nil, append([]Option{WithBreaker(br)}, opts...)...)

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
	sse := "data: {\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\ndata: [DONE]\n\n"
	slow := &stubUpstream{id: "slow", fn: func(_ context.Context, _ upstream.Request) (*upstream.Response, error) {
		time.Sleep(80 * time.Millisecond)
		resp := jsonResponse(t, http.StatusOK, sse)
		resp.Header.Set("Content-Type", "text/event-stream")
		return resp, nil
	}}
	exec := New(testPolicy(), nil, WithBreaker(br), WithSlowCallThreshold(20*time.Millisecond))

	execute(t, exec, []upstream.Upstream{slow}, true, `{"model":"m","stream":true,"messages":[]}`)
	if got := br.last(); got != circuit.OutcomeSlow {
		t.Fatalf("reported outcome = %v, want slow from the TTFT", got)
	}
}
