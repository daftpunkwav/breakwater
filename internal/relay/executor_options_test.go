/**
 * @file executor_options_test
 * @description Executor option wiring: WithClassifier replaces the
 * retryability table, visibly changing the attempt loop's decisions.
 * (WithBreaker, WithMetrics and WithStreamTimeout are exercised by the
 * failover, gateway-error-rendering and stream-time-budget files.)
 */
package relay

import (
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/daftpunkwav/breakwater/internal/upstream"
)

// neverRetry classifies every failure as terminal.
type neverRetry struct{}

func (neverRetry) Retryable(error) bool { return false }

func TestWithClassifierOverridesRetryability(t *testing.T) {
	t.Parallel()
	calls := 0
	cand := countedStubUpstream(t, &calls, "a", http.StatusInternalServerError, `{}`)
	exec := New(testPolicy(), nil, WithClassifier(neverRetry{}))

	result := execute(t, exec, []upstream.Upstream{cand}, false, "{}")
	if result.Status != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 passthrough", result.Status)
	}
	if calls != 1 {
		t.Fatalf("attempts = %d, want 1: the custom classifier must disable the retry", calls)
	}
}

// TestWithUpstreamBulkheadIgnoresAnUnusableGate pins the option's
// degenerate-argument guard: a nil gate or a limit at or below zero
// installs nothing. An unusable ceiling must degrade to unbounded
// upstream concurrency — never to a gate that refuses every attempt or
// a nil dereference on the first one.
func TestWithUpstreamBulkheadIgnoresAnUnusableGate(t *testing.T) {
	t.Parallel()
	for name, opt := range map[string]Option{
		"nil gate":       WithUpstreamBulkhead(nil, 5),
		"zero limit":     WithUpstreamBulkhead(newTestGate(), 0),
		"negative limit": WithUpstreamBulkhead(newTestGate(), -1),
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			exec := New(testPolicy(), nil, opt)
			if exec.bulkhead != nil || exec.bulkheadMax != 0 {
				t.Fatalf("bulkhead = %v max = %d, want nothing installed", exec.bulkhead, exec.bulkheadMax)
			}
		})
	}
}

// hintedClassifier is a classifier carrying the optional DelayHint
// extension the retry loop looks for.
type hintedClassifier struct{ hint time.Duration }

func (hintedClassifier) Retryable(error) bool { return true }

func (c hintedClassifier) DelayHint(error) time.Duration { return c.hint }

// TestRunClassifierForwardsTheDelayHint pins the decorator's optional
// extension: the retry loop finds DelayHint through a type assertion on
// the classifier it is handed, so the decorator must forward both the
// hint and its absence — a decorator that dropped the assertion would
// silently stop every Retry-After from being honored.
func TestRunClassifierForwardsTheDelayHint(t *testing.T) {
	t.Parallel()
	err := errors.New("exchange failed")
	run := &requestRun{} // the decorator's Retryable consults the run's rotation stash

	if got := (runClassifier{inner: hintedClassifier{hint: 7 * time.Second}, run: run}).DelayHint(err); got != 7*time.Second {
		t.Fatalf("DelayHint = %v, want the inner classifier's 7s hint forwarded", got)
	}
	// A classifier without the extension behaves as hint-less: zero,
	// and the loop keeps its computed backoff.
	if got := (runClassifier{inner: neverRetry{}, run: run}).DelayHint(err); got != 0 {
		t.Fatalf("DelayHint = %v, want 0 for a hint-less classifier", got)
	}
	if (runClassifier{inner: neverRetry{}, run: run}).Retryable(err) {
		t.Fatal("Retryable must forward to the inner classifier")
	}
}
