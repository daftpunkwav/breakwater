/**
 * @file fallback_test
 * @description The fallback plan: batches advance when candidates
 * exhaust, unresolvable models are skipped, cycles never re-resolve,
 * a committed stream ends the chain, and a hint never delays a
 * cross-model failover.
 */
package relay

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/daftpunkwav/breakwater/internal/obs"
	"github.com/daftpunkwav/breakwater/internal/retry"
	"github.com/daftpunkwav/breakwater/internal/upstream"
)

// scriptedUpstream answers every request with a fixed status and
// records the model of each exchange. retryAfter sets a Retry-After
// header; streamOverride replaces the body wholesale (for streams that
// die mid-flight).
type scriptedUpstream struct {
	id             string
	status         int
	models         []string
	streamOverride io.Reader
	retryAfter     string
}

func (s *scriptedUpstream) ID() string { return s.id }

func (s *scriptedUpstream) Forward(_ context.Context, req upstream.Request) (*upstream.Response, error) {
	s.models = append(s.models, req.Model)
	header := http.Header{}
	header.Set("Content-Type", "application/json")
	if s.retryAfter != "" {
		header.Set("Retry-After", s.retryAfter)
	}
	body := `{"error":{"type":"server_error"}}`
	if s.status >= 200 && s.status < 300 {
		body = `{"choices":[{"message":{"content":"hi"}}]}`
	}
	reader := io.Reader(strings.NewReader(body))
	if s.streamOverride != nil {
		reader = s.streamOverride
	}
	return &upstream.Response{
		StatusCode: s.status,
		Header:     header,
		Body:       io.NopCloser(reader),
	}, nil
}

func (s *scriptedUpstream) Probe(context.Context) error { return nil }

// executeJob runs a job against a recorder; the fallback tests need
// Job fields the plain execute helper does not set.
func executeJob(t *testing.T, exec *Executor, job Job) capturedResult {
	t.Helper()
	rec := httptest.NewRecorder()
	job.Out = rec
	result := exec.Execute(context.Background(), job)
	return capturedResult{Result: result, Body: rec.Body.Bytes(), Header: rec.Header()}
}

func TestFallbackAdvancesWhenCandidatesExhausted(t *testing.T) {
	t.Parallel()
	primary := &scriptedUpstream{id: "u1", status: http.StatusServiceUnavailable}
	fallback := &scriptedUpstream{id: "u2", status: http.StatusOK}
	resolved := []string{}
	exec := New(retry.Policy{MaxAttempts: 3}, nil)

	result := executeJob(t, exec, Job{
		Model:      "m1",
		Body:       []byte(`{}`),
		Candidates: []upstream.Upstream{primary},
		Fallbacks:  []string{"m2"},
		Resolve: func(_ context.Context, model string) ([]upstream.Upstream, error) {
			resolved = append(resolved, model)
			return []upstream.Upstream{fallback}, nil
		},
	})

	if result.Status != http.StatusOK || result.UpstreamID != "u2" {
		t.Fatalf("status = %d upstream = %q body = %s, want 200 from u2", result.Status, result.UpstreamID, result.Body)
	}
	if result.Attempts != 2 {
		t.Fatalf("attempts = %d, want 2 (one per batch)", result.Attempts)
	}
	if len(primary.models) != 1 || primary.models[0] != "m1" {
		t.Fatalf("primary saw models %v, want [m1]", primary.models)
	}
	if len(fallback.models) != 1 || fallback.models[0] != "m2" {
		t.Fatalf("fallback saw models %v, want [m2]: the body's model must ride the batch", fallback.models)
	}
	if len(resolved) != 1 || resolved[0] != "m2" {
		t.Fatalf("resolver called for %v, want [m2]", resolved)
	}
}

func TestFallbackSkipsUnresolvableModels(t *testing.T) {
	t.Parallel()
	primary := &scriptedUpstream{id: "u1", status: http.StatusServiceUnavailable}
	final := &scriptedUpstream{id: "u3", status: http.StatusOK}
	exec := New(retry.Policy{MaxAttempts: 3}, nil)

	result := executeJob(t, exec, Job{
		Model:      "m1",
		Body:       []byte(`{}`),
		Candidates: []upstream.Upstream{primary},
		Fallbacks:  []string{"m2", "m3"},
		Resolve: func(_ context.Context, model string) ([]upstream.Upstream, error) {
			if model == "m2" {
				return nil, errors.New("disabled")
			}
			return []upstream.Upstream{final}, nil
		},
	})

	if result.Status != http.StatusOK || result.UpstreamID != "u3" {
		t.Fatalf("status = %d upstream = %q, want 200 from u3 past the dead hop", result.Status, result.UpstreamID)
	}
	if result.Attempts != 2 {
		t.Fatalf("attempts = %d, want 2: a rejected hop consumes no attempt", result.Attempts)
	}
}

func TestFallbackCycleGuard(t *testing.T) {
	t.Parallel()
	primary := &scriptedUpstream{id: "u1", status: http.StatusServiceUnavailable}
	fallback := &scriptedUpstream{id: "u2", status: http.StatusServiceUnavailable}
	resolved := map[string]int{}
	exec := New(retry.Policy{MaxAttempts: 3}, nil)

	result := executeJob(t, exec, Job{
		Model:      "m1",
		Body:       []byte(`{}`),
		Candidates: []upstream.Upstream{primary},
		// The chain names the primary model again and duplicates its
		// own entry: neither may resolve twice.
		Fallbacks: []string{"m2", "m1", "m2"},
		Resolve: func(_ context.Context, model string) ([]upstream.Upstream, error) {
			resolved[model]++
			return []upstream.Upstream{fallback}, nil
		},
	})

	if result.Status != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want the 503 passthrough after the chain drains", result.Status)
	}
	if result.Attempts != 3 {
		t.Fatalf("attempts = %d, want 3 (u1, u2, clamp re-hit of u2)", result.Attempts)
	}
	if resolved["m1"] != 0 || resolved["m2"] != 1 {
		t.Fatalf("resolver calls = %v, want m2 once and m1 never", resolved)
	}
	if len(primary.models) != 1 || len(fallback.models) != 2 {
		t.Fatalf("exchanges = primary %v, fallback %v; want one on u1 and two on u2", primary.models, fallback.models)
	}
}

// abortReader yields one SSE frame then fails, simulating a stream
// that dies after the commit point.
type abortReader struct{ yielded bool }

func (a *abortReader) Read(p []byte) (int, error) {
	if a.yielded {
		return 0, errors.New("connection reset mid-stream")
	}
	a.yielded = true
	frame := "data: {\"id\":\"c1\"}\n\n"
	n := copy(p, frame)
	return n, nil
}

func TestFallbackStopsAfterStreamCommit(t *testing.T) {
	t.Parallel()
	primary := &scriptedUpstream{id: "u1", status: http.StatusOK}
	primary.streamOverride = &abortReader{}
	resolved := 0
	exec := New(retry.Policy{MaxAttempts: 3}, nil)

	result := executeJob(t, exec, Job{
		Model:      "m1",
		Stream:     true,
		Body:       []byte(`{}`),
		Candidates: []upstream.Upstream{primary},
		Fallbacks:  []string{"m2"},
		Resolve: func(context.Context, string) ([]upstream.Upstream, error) {
			resolved++
			return []upstream.Upstream{&scriptedUpstream{id: "u2", status: http.StatusOK}}, nil
		},
	})

	if !result.Aborted || result.Attempts != 1 {
		t.Fatalf("aborted = %v attempts = %d, want the committed stream to end the chain", result.Aborted, result.Attempts)
	}
	if resolved != 0 {
		t.Fatalf("resolver called %d times, want 0: no fallback after commit", resolved)
	}
}

func TestFallbackDoesNotWaitForHint(t *testing.T) {
	t.Parallel()
	primary := &scriptedUpstream{id: "u1", status: http.StatusTooManyRequests}
	primary.retryAfter = "30"
	fallback := &scriptedUpstream{id: "u2", status: http.StatusOK}
	exec := New(retry.Policy{MaxAttempts: 2}, nil)

	start := time.Now()
	result := executeJob(t, exec, Job{
		Model:      "m1",
		Body:       []byte(`{}`),
		Candidates: []upstream.Upstream{primary},
		Fallbacks:  []string{"m2"},
		Resolve: func(context.Context, string) ([]upstream.Upstream, error) {
			return []upstream.Upstream{fallback}, nil
		},
	})

	if result.Status != http.StatusOK || result.UpstreamID != "u2" {
		t.Fatalf("status = %d upstream = %q, want 200 from u2", result.Status, result.UpstreamID)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("failover took %v; the failed candidate's 30s hint must not delay another model", elapsed)
	}
}

// TestStreamPublishesTTFT: a committed stream records its
// time-to-first-byte on the per-upstream histogram, observable in the
// exposition alongside the attempt's other metrics.
func TestStreamPublishesTTFT(t *testing.T) {
	t.Parallel()
	primary := &scriptedUpstream{id: "u1", status: http.StatusOK}
	primary.streamOverride = strings.NewReader("data: {\"id\":\"c1\"}\n\ndata: [DONE]\n\n")
	metrics := obs.NewMetrics()
	exec := New(retry.Policy{MaxAttempts: 1}, nil, WithMetrics(metrics))

	result := executeJob(t, exec, Job{
		Model:      "m1",
		Stream:     true,
		Body:       []byte(`{}`),
		Candidates: []upstream.Upstream{primary},
	})

	if result.Status != http.StatusOK || result.Aborted {
		t.Fatalf("status = %d aborted = %v, want a clean stream", result.Status, result.Aborted)
	}
	var out strings.Builder
	if err := metrics.Render(&out); err != nil {
		t.Fatalf("render: %v", err)
	}
	if !strings.Contains(out.String(), `breakwater_upstream_ttft_seconds_count{upstream="u1"} 1`) {
		t.Fatalf("exposition missing the ttft sample:\n%s", out.String())
	}
}

// TestFallbackNilResolverClamps: a chain without a resolver is inert —
// extra attempts keep hitting the last candidate of the primary batch.
func TestFallbackNilResolverClamps(t *testing.T) {
	t.Parallel()
	primary := &scriptedUpstream{id: "u1", status: http.StatusServiceUnavailable}
	exec := New(retry.Policy{MaxAttempts: 3}, nil)

	result := executeJob(t, exec, Job{
		Model:      "m1",
		Body:       []byte(`{}`),
		Candidates: []upstream.Upstream{primary},
		Fallbacks:  []string{"m2"},
	})

	if result.Status != http.StatusServiceUnavailable || result.Attempts != 3 {
		t.Fatalf("status = %d attempts = %d, want 3 clamped attempts on u1", result.Status, result.Attempts)
	}
	if len(primary.models) != 3 {
		t.Fatalf("primary exchanges = %d, want 3", len(primary.models))
	}
}
