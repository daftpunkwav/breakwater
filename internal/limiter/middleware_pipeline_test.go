/**
 * @file middleware_pipeline_test
 * @description The rate limiting pipeline stage in isolation: carrier
 * precondition, the single body read rejections, the fail-closed
 * posture, the 429 contract (Retry-After + counter) and the post-call
 * refund correction.
 */
package limiter

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/daftpunkwav/breakwater/internal/auth"
	"github.com/daftpunkwav/breakwater/internal/obs"
	"github.com/daftpunkwav/breakwater/internal/pipeline"
	"github.com/daftpunkwav/breakwater/internal/protocol"
)

// stubLimiter scripts one Allow outcome and records every reservation
// and refund the stage performs.
type stubLimiter struct {
	decision    Decision
	allowErr    error
	allowCalls  int
	allowTenant string
	allowLimits Limits
	allowTokens int64
	refunds     []int64
}

func (s *stubLimiter) Allow(_ context.Context, tenantID string, limits Limits, tokens int64) (Decision, error) {
	s.allowCalls++
	s.allowTenant, s.allowLimits, s.allowTokens = tenantID, limits, tokens
	if s.allowErr != nil {
		return Decision{}, s.allowErr
	}
	return s.decision, nil
}

func (s *stubLimiter) Refund(_ context.Context, _ string, _ Limits, tokens int64) error {
	s.refunds = append(s.refunds, tokens)
	return nil
}

// failingBody is a request body whose read fails midway; it is neither
// too large nor malformed, just unreadable.
type failingBody struct{}

func (failingBody) Read([]byte) (int, error) { return 0, errors.New("read: socket reset") }

// limiterTenant is the identity the stage should see on every check.
var limiterTenant = auth.Tenant{ID: "t1", Tier: auth.Tier{RPM: 60, TPM: 60000}}

// serveThroughLimiter runs one request through the stage over a
// terminal handler that marks the tokens actually consumed. It returns
// the response, the carrier the terminal stage saw and how often the
// chain continued.
func serveThroughLimiter(t *testing.T, l Limiter, metrics *obs.Metrics, req *http.Request, consumed int64) (*httptest.ResponseRecorder, *pipeline.Carrier, int) {
	t.Helper()
	var nextCalls int
	var seen *pipeline.Carrier
	terminal := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		nextCalls++
		seen = pipeline.CarrierFrom(r.Context())
		if seen != nil {
			seen.Consumed = consumed
		}
		w.WriteHeader(http.StatusOK)
	})
	rec := httptest.NewRecorder()
	Middleware(l, metrics)(terminal).ServeHTTP(rec, req)
	return rec, seen, nextCalls
}

// chatRequest builds a carrier-backed chat request, as the chain entry
// and the earlier stages would have left it for the limiter stage.
func chatRequest(t *testing.T, tenant auth.Tenant, body io.Reader) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", body)
	carrier := &pipeline.Carrier{Tenant: tenant, Format: protocol.FormatOpenAIChat}
	return req.WithContext(pipeline.WithCarrier(req.Context(), carrier))
}

const threeWordBody = `{"model":"m","messages":[{"role":"user","content":"one two three"}]}`

// The three-word prompt plus the default 256-token completion reserve.
const wantEstimate = int64(259)

func TestLimiterMiddlewareRejectsWithoutCarrier(t *testing.T) {
	t.Parallel()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(threeWordBody))
	rec, _, nextCalls := serveThroughLimiter(t, &stubLimiter{}, nil, req, 0)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "pipeline_misconfigured") {
		t.Fatalf("body = %s", rec.Body.String())
	}
	if nextCalls != 0 {
		t.Fatal("a misconfigured chain must not reach the terminal stage")
	}
}

func TestLimiterMiddlewarePassesReservationAndRefundsRest(t *testing.T) {
	t.Parallel()
	req := chatRequest(t, limiterTenant, strings.NewReader(threeWordBody))
	lim := &stubLimiter{decision: Decision{Allowed: true}}
	rec, carrier, nextCalls := serveThroughLimiter(t, lim, nil, req, 57)

	if rec.Code != http.StatusOK || nextCalls != 1 {
		t.Fatalf("status = %d next = %d, want the chain to continue", rec.Code, nextCalls)
	}
	if lim.allowCalls != 1 || lim.allowTenant != "t1" {
		t.Fatalf("allow = %d calls for %q", lim.allowCalls, lim.allowTenant)
	}
	if lim.allowTokens != wantEstimate {
		t.Fatalf("reserved tokens = %d, want %d", lim.allowTokens, wantEstimate)
	}
	if lim.allowLimits != (Limits{RPM: 60, TPM: 60000}) {
		t.Fatalf("limits = %+v, want the tenant tier", lim.allowLimits)
	}
	if carrier == nil || carrier.Tokens != wantEstimate {
		t.Fatalf("carrier tokens not pinned to the estimate: %+v", carrier)
	}
	if len(lim.refunds) != 1 || lim.refunds[0] != wantEstimate-57 {
		t.Fatalf("refunds = %v, want one refund of %d", lim.refunds, wantEstimate-57)
	}
}

func TestLimiterMiddlewareFullConsumptionSkipsRefund(t *testing.T) {
	t.Parallel()
	req := chatRequest(t, limiterTenant, strings.NewReader(threeWordBody))
	lim := &stubLimiter{decision: Decision{Allowed: true}}
	serveThroughLimiter(t, lim, nil, req, wantEstimate)

	if len(lim.refunds) != 0 {
		t.Fatalf("refunds = %v, want none when consumption matches the estimate", lim.refunds)
	}
}

func TestLimiterMiddlewareClampsEstimateToTierMaxTokens(t *testing.T) {
	t.Parallel()
	tenant := limiterTenant
	tenant.Tier.MaxTokens = 100
	req := chatRequest(t, tenant, strings.NewReader(threeWordBody))
	lim := &stubLimiter{decision: Decision{Allowed: true}}
	serveThroughLimiter(t, lim, nil, req, 0)

	if lim.allowTokens != 103 { // 3 prompt words + the 100-token clamp
		t.Fatalf("reserved tokens = %d, want the clamped 103", lim.allowTokens)
	}
}

func TestLimiterMiddlewareFailsClosedOnBackendError(t *testing.T) {
	t.Parallel()
	req := chatRequest(t, limiterTenant, strings.NewReader(threeWordBody))
	lim := &stubLimiter{allowErr: errors.New("redis: connection refused")}
	rec, _, nextCalls := serveThroughLimiter(t, lim, nil, req, 0)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want fail-closed 503", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "governance_unavailable") {
		t.Fatalf("body = %s", rec.Body.String())
	}
	if nextCalls != 0 || len(lim.refunds) != 0 {
		t.Fatal("a gateway that cannot limit must not forward, and nothing was reserved")
	}
}

func TestLimiterMiddlewareRejects429WithRetryAfter(t *testing.T) {
	t.Parallel()
	req := chatRequest(t, limiterTenant, strings.NewReader(threeWordBody))
	metrics := obs.NewMetrics()
	lim := &stubLimiter{decision: Decision{Allowed: false, RetryAfter: 2500 * time.Millisecond}}
	rec, _, nextCalls := serveThroughLimiter(t, lim, metrics, req, 0)

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	if got := rec.Header().Get("Retry-After"); got != "2" {
		t.Fatalf("retry-after = %q, want 2 (2500ms truncated to seconds)", got)
	}
	if !strings.Contains(rec.Body.String(), string(protocol.CodeRateLimited)) {
		t.Fatalf("body = %s", rec.Body.String())
	}
	if nextCalls != 0 || len(lim.refunds) != 0 {
		t.Fatal("a rejected request must not reach the upstream or refund anything")
	}
	var rendered bytes.Buffer
	_ = metrics.Render(&rendered)
	if !strings.Contains(rendered.String(), `breakwater_rate_limited_total{tenant="t1"} 1`) {
		t.Fatalf("rejection counter not recorded:\n%s", rendered.String())
	}
}

func TestLimiterMiddlewareRetryAfterClampsToOneSecond(t *testing.T) {
	t.Parallel()
	for _, retryAfter := range []time.Duration{0, 300 * time.Millisecond, 999 * time.Millisecond} {
		req := chatRequest(t, limiterTenant, strings.NewReader(threeWordBody))
		lim := &stubLimiter{decision: Decision{Allowed: false, RetryAfter: retryAfter}}
		rec, _, _ := serveThroughLimiter(t, lim, nil, req, 0)

		if got := rec.Header().Get("Retry-After"); got != "1" {
			t.Fatalf("retry-after %v rendered as %q, want the 1s minimum", retryAfter, got)
		}
	}
}

func TestLimiterMiddlewareRejectsOversizedBody413(t *testing.T) {
	t.Parallel()
	body := bytes.Repeat([]byte("a"), protocol.MaxBodyBytes+1)
	req := chatRequest(t, limiterTenant, bytes.NewReader(body))
	lim := &stubLimiter{decision: Decision{Allowed: true}}
	rec, _, nextCalls := serveThroughLimiter(t, lim, nil, req, 0)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "request_too_large") {
		t.Fatalf("body = %s", rec.Body.String())
	}
	if lim.allowCalls != 0 || nextCalls != 0 {
		t.Fatal("an unread body must be rejected before any reservation")
	}
}

func TestLimiterMiddlewareRejectsUnreadableBody400(t *testing.T) {
	t.Parallel()
	req := chatRequest(t, limiterTenant, failingBody{})
	lim := &stubLimiter{decision: Decision{Allowed: true}}
	rec, _, _ := serveThroughLimiter(t, lim, nil, req, 0)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "invalid_request") {
		t.Fatalf("body = %s", rec.Body.String())
	}
	if lim.allowCalls != 0 {
		t.Fatal("no reservation may happen for an unreadable body")
	}
}

func TestLimiterMiddlewareRejectsMalformedBody400(t *testing.T) {
	t.Parallel()
	req := chatRequest(t, limiterTenant, strings.NewReader("{not json"))
	lim := &stubLimiter{decision: Decision{Allowed: true}}
	rec, _, _ := serveThroughLimiter(t, lim, nil, req, 0)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "invalid_request") {
		t.Fatalf("body = %s", rec.Body.String())
	}
	if lim.allowCalls != 0 {
		t.Fatal("no reservation may happen for an unparseable body")
	}
}
