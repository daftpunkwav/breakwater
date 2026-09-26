/**
 * @file fatal_test
 * @description Fatal upstream conditions and Retry-After travel: the
 * classification table, the hook firing exactly on fatal exchanges,
 * the hint parsed at the stash site, and the hint applying only to
 * same-upstream retries.
 */
package relay

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/daftpunkwav/breakwater/internal/retry"
	"github.com/daftpunkwav/breakwater/internal/upstream"
)

func TestFatalUpstreamReasonTable(t *testing.T) {
	t.Parallel()
	quotaBody := `{"error":{"code":"insufficient_quota","type":"insufficient_quota","message":"bill"}}`
	rateBody := `{"error":{"code":"rate_limit_exceeded","type":"rate_limit_error","message":"slow down"}}`
	cases := []struct {
		name   string
		status int
		body   string
		want   string
	}{
		{"quota exhaustion rides a 429", http.StatusTooManyRequests, quotaBody, reasonQuotaExhausted},
		{"a plain rate limit is transient", http.StatusTooManyRequests, rateBody, ""},
		{"a rate limit without a body is transient", http.StatusTooManyRequests, ``, ""},
		{"unauthorized is fatal", http.StatusUnauthorized, `{"error":{"type":"invalid_request_error"}}`, reasonAuthFailure},
		{"unauthorized without a body is fatal", http.StatusUnauthorized, ``, reasonAuthFailure},
		{"forbidden with an API envelope is fatal", http.StatusForbidden, `{"error":{"code":"forbidden","type":"permission_error"}}`, reasonAuthFailure},
		{"forbidden from a proxy page is transient", http.StatusForbidden, `<html>blocked</html>`, ""},
		{"the quota rule wins over the status rules", http.StatusInternalServerError, quotaBody, reasonQuotaExhausted},
		{"bad request is transient", http.StatusBadRequest, `{"error":{"code":400}}`, ""},
	}
	for _, tc := range cases {
		if got := fatalUpstreamReason(tc.status, []byte(tc.body)); got != tc.want {
			t.Errorf("%s: reason = %q, want %q", tc.name, got, tc.want)
		}
	}
}

// TestExecutorReportsFatalOncePerExchange: a 401 passthrough fires the
// hook with the auth reason; the exchange itself stays terminal.
func TestExecutorReportsFatalOncePerExchange(t *testing.T) {
	t.Parallel()
	var reports []string
	up := &stubUpstream{id: "u1", fn: func(context.Context, upstream.Request) (*upstream.Response, error) {
		return jsonResponse(t, http.StatusUnauthorized, `{"error":{"type":"authentication_error"}}`), nil
	}}
	exec := New(testPolicy(), nil, WithUpstreamFatalHook(func(id, reason string) {
		reports = append(reports, id+"/"+reason)
	}))
	result := execute(t, exec, []upstream.Upstream{up}, false, `{}`)
	if result.Status != http.StatusUnauthorized {
		t.Fatalf("status = %d, want the 401 passthrough", result.Status)
	}
	if len(reports) != 1 || reports[0] != "u1/upstream_auth_failure" {
		t.Fatalf("reports = %v, want exactly u1/upstream_auth_failure", reports)
	}
}

func TestExecutorKeepsQuietOnTransientFailures(t *testing.T) {
	t.Parallel()
	hookFired := false
	up := &stubUpstream{id: "u1", fn: func(_ context.Context, _ upstream.Request) (*upstream.Response, error) {
		return jsonResponse(t, http.StatusTooManyRequests,
			`{"error":{"code":"rate_limit_exceeded","type":"rate_limit_error"}}`), nil
	}}
	exec := New(testPolicy(), nil, WithUpstreamFatalHook(func(string, string) { hookFired = true }))
	result := execute(t, exec, []upstream.Upstream{up}, false, `{}`)
	if result.Status != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want the 429 passthrough", result.Status)
	}
	if hookFired {
		t.Fatal("a transient rate limit must not fire the fatal hook")
	}
}

// TestExecutorReportsQuotaExhaustionDespiteRetryability: a 429 with an
// insufficient_quota envelope is retryable by the table (the request
// may still fail over) and fatal at the same time — one hook report
// per observed exchange.
func TestExecutorReportsQuotaExhaustionDespiteRetryability(t *testing.T) {
	t.Parallel()
	var reasons []string
	up := &stubUpstream{id: "u1", fn: func(context.Context, upstream.Request) (*upstream.Response, error) {
		return jsonResponse(t, http.StatusTooManyRequests,
			`{"error":{"code":"insufficient_quota","type":"insufficient_quota"}}`), nil
	}}
	exec := New(testPolicy(), nil, WithUpstreamFatalHook(func(_ string, reason string) {
		reasons = append(reasons, reason)
	}))
	result := execute(t, exec, []upstream.Upstream{up}, false, `{}`)
	if result.Status != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want the 429 passthrough", result.Status)
	}
	if len(reasons) != 3 {
		t.Fatalf("reasons = %v, want one report per of the 3 attempts", reasons)
	}
	for _, r := range reasons {
		if r != reasonQuotaExhausted {
			t.Fatalf("reason = %q, want %q", r, reasonQuotaExhausted)
		}
	}
}

// TestExecutorHintDelaysSameUpstreamRetry: with a single candidate the
// loop re-hits the same upstream, so its Retry-After governs the wait.
func TestExecutorHintDelaysSameUpstreamRetry(t *testing.T) {
	t.Parallel()
	up := &stubUpstream{id: "u1", fn: func(_ context.Context, _ upstream.Request) (*upstream.Response, error) {
		resp := jsonResponse(t, http.StatusTooManyRequests, `{"error":{"type":"rate_limit_error"}}`)
		resp.Header.Set("Retry-After", "0.15")
		return resp, nil
	}}
	// An unshaped backoff: the only wait available is the hint.
	exec := New(retry.Policy{MaxAttempts: 2}, nil)
	start := time.Now()
	result := execute(t, exec, []upstream.Upstream{up}, false, `{}`)
	if result.Status != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want the 429 passthrough", result.Status)
	}
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Fatalf("returned after %v, want the upstream's 150ms hint honored", elapsed)
	}
}

// TestExecutorHintStrippedOnFailover: the first upstream asks for a
// long wait, but the next attempt targets a different candidate — the
// hint must not delay the failover.
func TestExecutorHintStrippedOnFailover(t *testing.T) {
	t.Parallel()
	impatient := &stubUpstream{id: "slow", fn: func(_ context.Context, _ upstream.Request) (*upstream.Response, error) {
		resp := jsonResponse(t, http.StatusTooManyRequests, `{"error":{"type":"rate_limit_error"}}`)
		resp.Header.Set("Retry-After", "30")
		return resp, nil
	}}
	healthy := &stubUpstream{id: "fast", fn: func(_ context.Context, _ upstream.Request) (*upstream.Response, error) {
		return jsonResponse(t, http.StatusTooManyRequests, `{"error":{"type":"rate_limit_error"}}`), nil
	}}
	exec := New(testPolicy(), nil)
	start := time.Now()
	result := execute(t, exec, []upstream.Upstream{impatient, healthy}, false, `{}`)
	if result.Status != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want the 429 passthrough", result.Status)
	}
	if result.Attempts != 3 {
		t.Fatalf("attempts = %d, want 3 (failover then re-hit of the last candidate)", result.Attempts)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("failover took %v; the failed candidate's 30s hint must not delay another upstream", elapsed)
	}
}

// TestStashParsesRetryAfterHeader pins the parsing at the stash site:
// integer seconds and HTTP-dates land on the status error, garbage and
// absence mean no hint.
func TestStashParsesRetryAfterHeader(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		header string
		set    bool
		want   time.Duration
	}{
		{"integer seconds", "5", true, 5 * time.Second},
		{"explicit zero", "0", true, 0},
		{"absent", "", false, 0},
		{"garbage", "junk", true, 0},
	}
	for _, tc := range cases {
		resp := jsonResponse(t, http.StatusTooManyRequests, `{}`)
		if tc.set {
			resp.Header.Set("Retry-After", tc.header)
		}
		run := &run{exec: New(testPolicy(), nil)}
		_, err := run.stashUpstreamError(&stubUpstream{id: "u1"}, resp, []byte(`{}`))
		statusErr := &retry.StatusError{}
		if !errors.As(err, &statusErr) {
			t.Fatalf("%s: err = %v, want a StatusError", tc.name, err)
		}
		if statusErr.RetryAfter != tc.want {
			t.Errorf("%s: hint = %v, want %v", tc.name, statusErr.RetryAfter, tc.want)
		}
	}

	// The HTTP-date form yields a positive hint.
	resp := jsonResponse(t, http.StatusTooManyRequests, `{}`)
	resp.Header.Set("Retry-After", time.Now().Add(2*time.Second).UTC().Format("Mon, 02 Jan 2006 15:04:05 GMT"))
	run := &run{exec: New(testPolicy(), nil)}
	_, err := run.stashUpstreamError(&stubUpstream{id: "u1"}, resp, []byte(`{}`))
	statusErr := &retry.StatusError{}
	if !errors.As(err, &statusErr) || statusErr.RetryAfter == 0 {
		t.Fatalf("HTTP-date hint = %v, err = %v; want a positive hint", statusErr.RetryAfter, err)
	}
}
