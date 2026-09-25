/**
 * @file gateway_error_rendering_test
 * @description The gateway's own error envelopes produced by finish():
 * circuit-open and budget-exhausted 503s, the upstream-unreachable
 * fallback, and the client-disconnect branch that reports the intended
 * status without writing to the dead connection.
 */
package relay

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/daftpunkwav/breakwater/internal/circuit"
	"github.com/daftpunkwav/breakwater/internal/retry"
	"github.com/daftpunkwav/breakwater/internal/upstream"
)

// denyBreaker refuses every call, pinning the executor against the
// fully-open condition without depending on the real state machine.
type denyBreaker struct{}

func (denyBreaker) Allow(context.Context, string) (circuit.Permission, bool) { return nil, false }

func (denyBreaker) StateOf(context.Context, string) circuit.State { return circuit.StateOpen }

// closedBreaker grants everything and absorbs reports, exercising the
// grant branch of the attempt's breaker accounting.
type closedBreaker struct{}

func (closedBreaker) Allow(context.Context, string) (circuit.Permission, bool) {
	return nopPermission{}, true
}

func (closedBreaker) StateOf(context.Context, string) circuit.State { return circuit.StateClosed }

type nopPermission struct{}

func (nopPermission) Report(circuit.Outcome) {}

func TestCircuitOpenRendersServiceUnavailable(t *testing.T) {
	t.Parallel()
	cand := &stubUpstream{id: "a", fn: func(context.Context, upstream.Request) (*upstream.Response, error) {
		t.Error("upstream touched while the breaker was open")
		return nil, errors.New("unreachable")
	}}
	exec := New(testPolicy(), nil, WithBreaker(denyBreaker{}))

	result := execute(t, exec, []upstream.Upstream{cand}, false, "{}")
	if result.Status != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", result.Status)
	}
	if !strings.Contains(string(result.Body), `"code":"circuit_open"`) {
		t.Fatalf("body = %q, want circuit_open envelope", result.Body)
	}
	if result.Attempts != 3 {
		t.Fatalf("attempts = %d, want 3: every attempt must meet the breaker", result.Attempts)
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

// TestUnreachableUpstreamRendersGatewayError pins the fallback branch:
// a transport-level failure that survives the attempt loop renders as
// the upstream_unreachable 502.
func TestUnreachableUpstreamRendersGatewayError(t *testing.T) {
	t.Parallel()
	cand := &stubUpstream{id: "a", fn: func(context.Context, upstream.Request) (*upstream.Response, error) {
		return nil, errors.New("connection refused")
	}}
	exec := New(retry.Policy{MaxAttempts: 1}, nil)

	result := execute(t, exec, []upstream.Upstream{cand}, false, "{}")
	if result.Status != http.StatusBadGateway {
		t.Fatalf("status = %d, want 502", result.Status)
	}
	if !strings.Contains(string(result.Body), `"code":"upstream_unreachable"`) {
		t.Fatalf("body = %q, want gateway envelope", result.Body)
	}
}

// TestClientDisconnectBeforeFirstByteReportsIntendedStatus pins the
// client-gone branch: nothing is written to the dead connection, and
// the intended status falls back to 502 for transport failures.
func TestClientDisconnectBeforeFirstByteReportsIntendedStatus(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cand := &stubUpstream{id: "a", fn: func(ctx context.Context, _ upstream.Request) (*upstream.Response, error) {
		return nil, ctx.Err()
	}}
	rec := newDiscardingRecorder()
	exec := New(retry.Policy{MaxAttempts: 1}, nil)

	result := exec.Execute(ctx, Job{Model: "m", Candidates: []upstream.Upstream{cand}, Out: rec})
	if !result.ClientGone {
		t.Fatal("client disconnect not reported")
	}
	if result.Status != http.StatusBadGateway {
		t.Fatalf("status = %d, want the intended 502", result.Status)
	}
	if len(rec.body) != 0 {
		t.Fatalf("bytes written to a disconnected client: %q", rec.body)
	}
}

// TestClientDisconnectWithTerminalErrorReportsTerminalStatus pins the
// intendedStatus composition: a disconnect racing a terminal upstream
// rejection reports the upstream status for observation.
func TestClientDisconnectWithTerminalErrorReportsTerminalStatus(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cand := &stubUpstream{id: "a", fn: func(_ context.Context, _ upstream.Request) (*upstream.Response, error) {
		// The client walks away while the upstream rejection lands.
		cancel()
		return jsonResponse(t, http.StatusUnauthorized, `{"error":{}}`), nil
	}}
	rec := newDiscardingRecorder()
	exec := New(retry.Policy{MaxAttempts: 2}, nil)

	result := exec.Execute(ctx, Job{Model: "m", Candidates: []upstream.Upstream{cand}, Out: rec})
	if !result.ClientGone {
		t.Fatal("client disconnect not reported")
	}
	if result.Status != http.StatusUnauthorized {
		t.Fatalf("status = %d, want the terminal 401", result.Status)
	}
	if len(rec.body) != 0 {
		t.Fatalf("bytes written to a disconnected client: %q", rec.body)
	}
}

// TestIntendedStatusMapsEveryFailureClass pins the white-box mapping
// from the final loop error to the status the client would have seen —
// including the classes the disconnect race cannot reach through
// Execute alone.
func TestIntendedStatusMapsEveryFailureClass(t *testing.T) {
	t.Parallel()
	statusErr := retry.NewStatusError(http.StatusTooManyRequests)
	r := &run{
		terminal:      snapshot(http.StatusUnauthorized, http.Header{}, []byte(`{}`)),
		lastFailed:    snapshot(http.StatusTooManyRequests, http.Header{}, []byte(`{}`)),
		lastFailedErr: statusErr,
	}

	if got := r.intendedStatus(context.Canceled); got != http.StatusUnauthorized {
		t.Fatalf("terminal mapping = %d, want 401", got)
	}
	r.terminal = nil
	if got := r.intendedStatus(errCircuitOpen); got != http.StatusServiceUnavailable {
		t.Fatalf("circuit-open mapping = %d, want 503", got)
	}
	if got := r.intendedStatus(retry.ErrBudgetExhausted); got != http.StatusServiceUnavailable {
		t.Fatalf("budget mapping = %d, want 503", got)
	}
	if got := r.intendedStatus(statusErr); got != http.StatusTooManyRequests {
		t.Fatalf("last-failed mapping = %d, want 429", got)
	}
	r.lastFailed = nil
	if got := r.intendedStatus(errors.New("connection refused")); got != http.StatusBadGateway {
		t.Fatalf("fallback mapping = %d, want 502", got)
	}
}

// TestBreakerGrantReportsOutcome exercises the granted branch of the
// breaker accounting: the attempt runs and reports through the
// permission without effect on the reply.
func TestBreakerGrantReportsOutcome(t *testing.T) {
	t.Parallel()
	cand := &stubUpstream{id: "a", fn: func(context.Context, upstream.Request) (*upstream.Response, error) {
		return jsonResponse(t, http.StatusOK, `{"usage":{"total_tokens":1}}`), nil
	}}
	exec := New(testPolicy(), nil, WithBreaker(closedBreaker{}))

	result := execute(t, exec, []upstream.Upstream{cand}, false, "{}")
	if result.Status != http.StatusOK || result.UpstreamID != "a" {
		t.Fatalf("status = %d upstream = %q, want 200/a under a granting breaker", result.Status, result.UpstreamID)
	}
}

// discardingRecorder captures whether finish wrote anything without
// pretending the client still reads.
type discardingRecorder struct {
	header http.Header
	body   []byte
}

func newDiscardingRecorder() *discardingRecorder {
	return &discardingRecorder{header: http.Header{}}
}

func (d *discardingRecorder) Header() http.Header { return d.header }

func (d *discardingRecorder) Write(p []byte) (int, error) {
	d.body = append(d.body, p...)
	return len(p), nil
}

func (d *discardingRecorder) WriteHeader(int) {}
