/**
 * @file bulkhead_test
 * @description The per-upstream in-flight ceiling as the attempt loop
 * sees it: a saturated candidate is skipped toward the next one, the
 * refusal never reaches the breaker, and the slot returns when the
 * attempt ends whatever its outcome.
 */
package relay

import (
	"context"
	"net/http"
	"sync"
	"testing"

	"github.com/daftpunkwav/breakwater/internal/circuit"
	"github.com/daftpunkwav/breakwater/internal/retry"
	"github.com/daftpunkwav/breakwater/internal/upstream"
)

// testGate is the Bulkhead port's test double: the same non-blocking
// counting semantics with an idempotent release, no implementation
// package behind it (importing the real one from this package's tests
// would close a cycle through the pipeline).
type testGate struct {
	mu sync.Mutex
	n  map[string]int64
}

func newTestGate() *testGate { return &testGate{n: make(map[string]int64)} }

func (g *testGate) Acquire(id string, limit int64) (func(), bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.n[id] >= limit {
		return nil, false
	}
	g.n[id]++
	released := false
	return func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		if !released {
			released = true
			g.n[id]--
		}
	}, true
}

func (g *testGate) inFlight(id string) int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.n[id]
}

func okUpstream(t *testing.T, id string) *stubUpstream {
	t.Helper()
	return &stubUpstream{id: id, fn: func(_ context.Context, _ upstream.Request) (*upstream.Response, error) {
		return jsonResponse(t, http.StatusOK, `{"choices":[]}`), nil
	}}
}

func TestBulkheadSkipsSaturatedCandidate(t *testing.T) {
	gate := newTestGate()
	// Hold the only slot of the first candidate from outside the
	// request, the way live traffic would.
	release, ok := gate.Acquire("first", 1)
	if !ok {
		t.Fatal("pre-acquire failed")
	}
	defer release()

	br := &outcomeRecorder{}
	exec := New(testPolicy(), nil, WithBreaker(br), WithUpstreamBulkhead(gate, 1))
	captured := execute(t, exec, []upstream.Upstream{okUpstream(t, "first"), okUpstream(t, "second")}, false, `{}`)

	if captured.Status != http.StatusOK || captured.UpstreamID != "second" {
		t.Fatalf("status = %d upstream = %q, want 200 on second", captured.Status, captured.UpstreamID)
	}
	// The skip is a capacity refusal, not a health event: the only
	// breaker report is the one exchange that actually ran.
	if got := len(br.snapshot()); got != 1 {
		t.Fatalf("breaker outcomes = %d, want 1 (the served exchange)", got)
	}
}

func TestBulkheadAllSaturatedFailsFast(t *testing.T) {
	gate := newTestGate()
	r1, ok1 := gate.Acquire("a", 1)
	r2, ok2 := gate.Acquire("b", 1)
	if !ok1 || !ok2 {
		t.Fatal("pre-acquire failed")
	}
	defer r1()
	defer r2()

	exec := New(testPolicy(), nil, WithUpstreamBulkhead(gate, 1))
	captured := execute(t, exec, []upstream.Upstream{okUpstream(t, "a"), okUpstream(t, "b")}, false, `{}`)

	if captured.Status != http.StatusServiceUnavailable || captured.ErrorCode != "upstream_saturated" {
		t.Fatalf("status = %d code = %q, want 503 upstream_saturated", captured.Status, captured.ErrorCode)
	}
	if captured.Attempts != 3 {
		t.Fatalf("attempts = %d, want 3 (both candidates tried, last clamped)", captured.Attempts)
	}
}

func TestBulkheadSlotReturnsAfterAttempt(t *testing.T) {
	gate := newTestGate()
	exec := New(testPolicy(), nil, WithUpstreamBulkhead(gate, 1))
	cands := []upstream.Upstream{okUpstream(t, "solo")}

	for i := 0; i < 2; i++ {
		captured := execute(t, exec, cands, false, `{}`)
		if captured.Status != http.StatusOK {
			t.Fatalf("request %d: status = %d, want 200 (slot must return)", i+1, captured.Status)
		}
	}
	if got := gate.inFlight("solo"); got != 0 {
		t.Fatalf("in-flight after success = %d, want 0", got)
	}
}

func TestBulkheadSlotReturnsOnFailedAttempt(t *testing.T) {
	gate := newTestGate()
	failing := &stubUpstream{id: "bad", fn: func(_ context.Context, _ upstream.Request) (*upstream.Response, error) {
		return jsonResponse(t, http.StatusInternalServerError, `{"error":{}}`), nil
	}}
	exec := New(retry.Policy{MaxAttempts: 2}, nil, WithUpstreamBulkhead(gate, 1))
	captured := execute(t, exec, []upstream.Upstream{failing}, false, `{}`)

	if captured.Status != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", captured.Status)
	}
	// The failed attempt's slot is back: a later request can acquire.
	release, ok := gate.Acquire("bad", 1)
	if !ok {
		t.Fatal("slot leaked by the failed attempt")
	}
	release()
}

func TestBulkheadAbsentByDefault(t *testing.T) {
	// No gate installed: an Executor keeps upstream concurrency
	// unbounded and every candidate is reachable.
	exec := New(testPolicy(), nil)
	captured := execute(t, exec, []upstream.Upstream{okUpstream(t, "a"), okUpstream(t, "b")}, false, `{}`)
	if captured.Status != http.StatusOK || captured.UpstreamID != "a" {
		t.Fatalf("status = %d upstream = %q, want 200 on a", captured.Status, captured.UpstreamID)
	}
}

// snapshot copies the recorded outcomes for assertion.
func (b *outcomeRecorder) snapshot() []circuit.Outcome {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]circuit.Outcome(nil), b.outcomes...)
}
