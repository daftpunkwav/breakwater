/**
 * @file middleware_stage_test
 * @description The quota pipeline stage in isolation: the reserve
 * decision (402 vs 503), the recompute fallback, and the settlement
 * split between cancel and settle with the refund metrics they emit.
 */
package quota

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/daftpunkwav/breakwater/internal/auth"
	"github.com/daftpunkwav/breakwater/internal/obs"
	"github.com/daftpunkwav/breakwater/internal/pipeline"
	"github.com/daftpunkwav/breakwater/internal/protocol"
)

// stubLedger scripts Reserve/Cancel/Settle outcomes and records every
// call the stage performs.
type stubLedger struct {
	lease         Lease
	reserveErr    error
	reserveCalls  int
	reserveTenant string
	reserveAmount int64

	settleCalls int
	settleID    string
	settleUsed  int64
	settleErr   error

	cancelCalls int
	cancelID    string
	cancelErr   error
}

func (s *stubLedger) Reserve(_ context.Context, tenantID string, amount int64) (Lease, error) {
	s.reserveCalls++
	s.reserveTenant, s.reserveAmount = tenantID, amount
	if s.reserveErr != nil {
		return Lease{}, s.reserveErr
	}
	return s.lease, nil
}

func (s *stubLedger) Settle(_ context.Context, leaseID string, usedTokens int64) error {
	s.settleCalls++
	s.settleID, s.settleUsed = leaseID, usedTokens
	return s.settleErr
}

func (s *stubLedger) Cancel(_ context.Context, leaseID string) error {
	s.cancelCalls++
	s.cancelID = leaseID
	return s.cancelErr
}

func (s *stubLedger) Balance(context.Context, string) (int64, error)  { return 0, nil }
func (s *stubLedger) SetBalance(context.Context, string, int64) error { return nil }

// quotaTenant is the identity the stage should attribute every movement
// to.
var quotaTenant = auth.Tenant{ID: "t1"}

// quotaRequest builds a carrier as the auth and limiter stages would
// have left it: estimated tokens pinned, chat request parsed.
func quotaRequest(t *testing.T, tokens int64) *http.Request {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	carrier := &pipeline.Carrier{
		Tenant: quotaTenant,
		Tokens: tokens,
		Chat: protocol.ChatRequest{
			Model:    "m",
			Messages: []protocol.ChatMessage{{Role: "user", Content: "one two three"}},
		},
	}
	return req.WithContext(pipeline.WithCarrier(req.Context(), carrier))
}

// serveThroughQuota runs one request through the stage over a terminal
// handler that reports the tokens actually consumed.
func serveThroughQuota(t *testing.T, ledger Ledger, metrics *obs.Metrics, req *http.Request, consumed int64) (*httptest.ResponseRecorder, *pipeline.Carrier, int) {
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
	Middleware(ledger, metrics)(terminal).ServeHTTP(rec, req)
	return rec, seen, nextCalls
}

func renderedMetric(t *testing.T, m *obs.Metrics, needle string) bool {
	t.Helper()
	var buf bytes.Buffer
	if err := m.Render(&buf); err != nil {
		t.Fatalf("render metrics: %v", err)
	}
	return strings.Contains(buf.String(), needle)
}

func TestQuotaMiddlewareRejectsWithoutCarrier(t *testing.T) {
	t.Parallel()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	rec, _, nextCalls := serveThroughQuota(t, &stubLedger{}, nil, req, 0)

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

func TestQuotaMiddlewareRecomputesWhenEstimateMissing(t *testing.T) {
	t.Parallel()
	// A deployment without the limiter stage: the quota stage derives
	// the estimate itself (3 prompt words + the 256-token default).
	req := quotaRequest(t, 0)
	ledger := &stubLedger{lease: Lease{ID: "lease-1"}}
	rec, carrier, nextCalls := serveThroughQuota(t, ledger, nil, req, 0)

	if rec.Code != http.StatusOK || nextCalls != 1 {
		t.Fatalf("status = %d next = %d, want the chain to continue", rec.Code, nextCalls)
	}
	if ledger.reserveCalls != 1 || ledger.reserveAmount != 259 {
		t.Fatalf("reserve amount = %d, want the recomputed 259", ledger.reserveAmount)
	}
	if ledger.reserveTenant != "t1" {
		t.Fatalf("reserve tenant = %q", ledger.reserveTenant)
	}
	if carrier == nil || carrier.Tokens != 259 {
		t.Fatalf("carrier estimate not pinned: %+v", carrier)
	}
}

func TestQuotaMiddlewareDenies402WhenBalanceInsufficient(t *testing.T) {
	t.Parallel()
	req := quotaRequest(t, 500)
	ledger := &stubLedger{reserveErr: ErrInsufficientBalance}
	rec, _, nextCalls := serveThroughQuota(t, ledger, nil, req, 0)

	if rec.Code != http.StatusPaymentRequired {
		t.Fatalf("status = %d, want 402", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), string(protocol.CodeInsufficientQuota)) {
		t.Fatalf("body = %s", rec.Body.String())
	}
	if nextCalls != 0 {
		t.Fatal("a drained tenant must not reach an upstream")
	}
}

func TestQuotaMiddlewareFailsClosedOnLedgerError(t *testing.T) {
	t.Parallel()
	req := quotaRequest(t, 500)
	ledger := &stubLedger{reserveErr: errors.New("redis: connection refused")}
	rec, _, nextCalls := serveThroughQuota(t, ledger, nil, req, 0)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want fail-closed 503", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "governance_unavailable") {
		t.Fatalf("body = %s", rec.Body.String())
	}
	if nextCalls != 0 {
		t.Fatal("a gateway that cannot meter must not give away traffic")
	}
}

func TestQuotaMiddlewareSettlesConsumptionAndRefundsRest(t *testing.T) {
	t.Parallel()
	req := quotaRequest(t, 500)
	metrics := obs.NewMetrics()
	ledger := &stubLedger{lease: Lease{ID: "lease-1"}}
	rec, _, _ := serveThroughQuota(t, ledger, metrics, req, 150)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if ledger.reserveCalls != 1 || ledger.cancelCalls != 0 {
		t.Fatalf("reserve = %d cancel = %d, want one reserve, no cancel", ledger.reserveCalls, ledger.cancelCalls)
	}
	if ledger.settleCalls != 1 || ledger.settleID != "lease-1" || ledger.settleUsed != 150 {
		t.Fatalf("settle = %d calls (%s, %d tokens), want lease-1 settled with 150",
			ledger.settleCalls, ledger.settleID, ledger.settleUsed)
	}
	if !renderedMetric(t, metrics, `breakwater_quota_reservation_tokens_total{tenant="t1"} 500`) {
		t.Fatal("reservation metric missing")
	}
	if !renderedMetric(t, metrics, `breakwater_quota_refunded_tokens_total{tenant="t1"} 350`) {
		t.Fatal("partial refund metric missing")
	}
}

func TestQuotaMiddlewareCancelsZeroConsumption(t *testing.T) {
	t.Parallel()
	req := quotaRequest(t, 500)
	metrics := obs.NewMetrics()
	ledger := &stubLedger{lease: Lease{ID: "lease-2"}}
	rec, _, _ := serveThroughQuota(t, ledger, metrics, req, 0)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if ledger.settleCalls != 0 || ledger.cancelCalls != 1 || ledger.cancelID != "lease-2" {
		t.Fatalf("settle = %d cancel = %d (%s), want a full release",
			ledger.settleCalls, ledger.cancelCalls, ledger.cancelID)
	}
	if !renderedMetric(t, metrics, `breakwater_quota_refunded_tokens_total{tenant="t1"} 500`) {
		t.Fatal("full refund metric missing")
	}
}

func TestQuotaMiddlewareExactConsumptionSkipsRefundMetric(t *testing.T) {
	t.Parallel()
	req := quotaRequest(t, 500)
	metrics := obs.NewMetrics()
	ledger := &stubLedger{lease: Lease{ID: "lease-3"}}
	serveThroughQuota(t, ledger, metrics, req, 500)

	if ledger.settleCalls != 1 || ledger.settleUsed != 500 {
		t.Fatalf("settle = %+v/%d, want a settle with the full estimate", ledger.settleID, ledger.settleUsed)
	}
	if renderedMetric(t, metrics, `breakwater_quota_refunded_tokens_total{tenant="t1"}`) {
		t.Fatal("a fully consumed estimate must not emit a refund metric")
	}
}

// recordingHandler captures the default logger's records so settlement
// failures can be asserted as surfaced, not swallowed.
type recordingHandler struct {
	records []string
}

func newRecordingHandler() *recordingHandler {
	return &recordingHandler{}
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.records = append(h.records, r.Message)
	return nil
}

func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler      { return h }

func TestQuotaMiddlewareLogsSettlementFailures(t *testing.T) {
	// The settlement writes detach from the request; the test keeps the
	// default logger swapped only for its own duration.
	handler := newRecordingHandler()
	prev := slog.Default()
	slog.SetDefault(slog.New(handler))
	t.Cleanup(func() { slog.SetDefault(prev) })

	// A failed cancel must not fail the response, and must be logged.
	req := quotaRequest(t, 500)
	ledger := &stubLedger{lease: Lease{ID: "lease-4"}, cancelErr: errors.New("ledger gone")}
	if rec, _, _ := serveThroughQuota(t, ledger, nil, req, 0); rec.Code != http.StatusOK {
		t.Fatalf("cancel failure changed the response: %d", rec.Code)
	}
	// A failed settle likewise.
	req = quotaRequest(t, 500)
	ledger = &stubLedger{lease: Lease{ID: "lease-5"}, settleErr: errors.New("ledger gone")}
	if rec, _, _ := serveThroughQuota(t, ledger, nil, req, 150); rec.Code != http.StatusOK {
		t.Fatalf("settle failure changed the response: %d", rec.Code)
	}

	joined := strings.Join(handler.records, "\n")
	if !strings.Contains(joined, "quota cancel failed") {
		t.Fatalf("cancel failure not logged: %v", handler.records)
	}
	if !strings.Contains(joined, "quota settle failed") {
		t.Fatalf("settle failure not logged: %v", handler.records)
	}
}
