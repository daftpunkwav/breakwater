/**
 * @file executor_options_test
 * @description Executor option wiring: WithClassifier replaces the
 * retryability table, visibly changing the attempt loop's decisions.
 * (WithBreaker, WithMetrics and WithStreamTimeout are exercised by the
 * failover, gateway-error-rendering and stream-time-budget files.)
 */
package relay

import (
	"context"
	"net/http"
	"testing"

	"github.com/daftpunkwav/breakwater/internal/upstream"
)

// neverRetry classifies every failure as terminal.
type neverRetry struct{}

func (neverRetry) Retryable(error) bool { return false }

func TestWithClassifierOverridesRetryability(t *testing.T) {
	t.Parallel()
	calls := 0
	cand := &stubUpstream{id: "a", fn: func(context.Context, upstream.Request) (*upstream.Response, error) {
		calls++
		return jsonResponse(t, http.StatusInternalServerError, `{"error":{}}`), nil
	}}
	exec := New(testPolicy(), nil, WithClassifier(neverRetry{}))

	result := execute(t, exec, []upstream.Upstream{cand}, false, "{}")
	if result.Status != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 passthrough", result.Status)
	}
	if calls != 1 {
		t.Fatalf("attempts = %d, want 1: the custom classifier must disable the retry", calls)
	}
}
