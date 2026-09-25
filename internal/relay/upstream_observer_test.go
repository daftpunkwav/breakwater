/**
 * @file upstream_observer_test
 * @description The upstream outcome observer: one report per attempt,
 * failures marked as such and client disconnects kept off the record.
 */
package relay

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/daftpunkwav/breakwater/internal/retry"
	"github.com/daftpunkwav/breakwater/internal/upstream"
)

// recordingObserver captures ObserveUpstream calls.
type recordingObserver struct {
	calls []outcomeCall
}

type outcomeCall struct {
	id      string
	failed  bool
	latency time.Duration
}

func (r *recordingObserver) ObserveUpstream(id string, latency time.Duration, failed bool) {
	r.calls = append(r.calls, outcomeCall{id: id, failed: failed, latency: latency})
}

func TestObserverReportsEveryAttempt(t *testing.T) {
	t.Parallel()
	primary := &stubUpstream{id: "primary", fn: func(context.Context, upstream.Request) (*upstream.Response, error) {
		return jsonResponse(t, http.StatusServiceUnavailable, `{}`), nil
	}}
	fallback := &stubUpstream{id: "fallback", fn: func(context.Context, upstream.Request) (*upstream.Response, error) {
		return jsonResponse(t, http.StatusOK, `{}`), nil
	}}
	obs := &recordingObserver{}
	exec := New(testPolicy(), nil, WithUpstreamObserver(obs))

	result := execute(t, exec, []upstream.Upstream{primary, fallback}, false, "{}")
	if result.Status != http.StatusOK {
		t.Fatalf("status = %d, want 200", result.Status)
	}
	if len(obs.calls) != 2 {
		t.Fatalf("observer saw %d calls, want one per attempt", len(obs.calls))
	}
	if obs.calls[0].id != "primary" || !obs.calls[0].failed {
		t.Fatalf("first call = %+v, want failed primary", obs.calls[0])
	}
	if obs.calls[1].id != "fallback" || obs.calls[1].failed {
		t.Fatalf("second call = %+v, want successful fallback", obs.calls[1])
	}
}

func TestObserverSparedOnClientDisconnect(t *testing.T) {
	t.Parallel()
	// The exchange fails with a canceled context: the client walked
	// away, which must not demote the upstream. The cancellation fires
	// mid-exchange from the "client side" of the test.
	slow := &stubUpstream{id: "slow", fn: func(ctx context.Context, _ upstream.Request) (*upstream.Response, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	obs := &recordingObserver{}
	exec := New(testPolicy(), nil, WithUpstreamObserver(obs))

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel()
	}()
	rec := httptest.NewRecorder()
	exec.Execute(ctx, Job{
		Model:      "m",
		Body:       []byte("{}"),
		Candidates: []upstream.Upstream{slow},
		Out:        rec,
	})
	cancel()

	if len(obs.calls) == 0 {
		t.Fatal("observer saw no calls")
	}
	for _, call := range obs.calls {
		if call.failed {
			t.Fatalf("call = %+v, want a canceled exchange kept off the failure record", call)
		}
	}
}

// TestObserverRecordsServerSourcedCancel pins the client-fault rule the
// observer shares with the breaker accounting (clientFault): a
// cancel-flavored exchange error while the client context is still
// alive is not the client's doing and must hit the failure record.
func TestObserverRecordsServerSourcedCancel(t *testing.T) {
	t.Parallel()
	flaky := &stubUpstream{id: "flaky", fn: func(context.Context, upstream.Request) (*upstream.Response, error) {
		return nil, fmt.Errorf("transport tore down: %w", context.Canceled)
	}}
	obs := &recordingObserver{}
	exec := New(retry.Policy{MaxAttempts: 1}, nil, WithUpstreamObserver(obs))

	rec := httptest.NewRecorder()
	exec.Execute(context.Background(), Job{
		Model:      "m",
		Body:       []byte("{}"),
		Candidates: []upstream.Upstream{flaky},
		Out:        rec,
	})
	if len(obs.calls) != 1 {
		t.Fatalf("observer saw %d calls, want one", len(obs.calls))
	}
	if !obs.calls[0].failed {
		t.Fatalf("call = %+v, want a non-client cancel recorded as a failure", obs.calls[0])
	}
}

func TestObserverNilDisables(t *testing.T) {
	t.Parallel()
	// The default executor has no observer; this exercises the nil
	// branch so coverage and honesty stay aligned.
	cand := &stubUpstream{id: "u", fn: func(context.Context, upstream.Request) (*upstream.Response, error) {
		return jsonResponse(t, http.StatusOK, `{}`), nil
	}}
	exec := New(testPolicy(), nil)
	result := execute(t, exec, []upstream.Upstream{cand}, false, "{}")
	if result.Status != http.StatusOK {
		t.Fatalf("status = %d, want 200", result.Status)
	}
}
