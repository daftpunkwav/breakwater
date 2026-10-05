/**
 * @file chain_test
 * @description The composed governance pipeline end to end: identity,
 * rate limiting and quota around the completions route — over-limit
 * rejection, drained-balance rejection, and the auth rejections.
 */
package server

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/daftpunkwav/breakwater/internal/auth"
	"github.com/daftpunkwav/breakwater/internal/limiter"
	"github.com/daftpunkwav/breakwater/internal/pipeline"
	"github.com/daftpunkwav/breakwater/internal/protocol"
	"github.com/daftpunkwav/breakwater/internal/quota"
)

const (
	keyT1 = "key-t1"
	keyT2 = "key-t2"
)

// buildChain assembles the full stage chain over the completions
// handler with in-memory backends and the shared test upstream.
func buildChain(t *testing.T, backendURL string, ledger quota.Ledger) http.Handler {
	t.Helper()
	identity := mustIdentity(t, auth.StaticConfig{
		Tiers: []auth.StaticTier{
			{ID: "free", RPM: 2, TPM: 10_000, MaxTokens: 50, MonthlyQuota: 100_000, AllowedModels: []string{"*"}},
		},
		Tenants: []auth.StaticTenant{
			{ID: "t1", Name: "Tenant One", Tier: "free", Keys: []string{keyT1}},
			{ID: "t2", Name: "Tenant Two", Tier: "free", Keys: []string{keyT2}},
		},
	})
	return buildChainWithIdentity(t, backendURL, ledger, identity)
}

// buildChainWithIdentity assembles the full governance chain over the
// given identity store.
func buildChainWithIdentity(t *testing.T, backendURL string, ledger quota.Ledger, identity auth.Store) http.Handler {
	t.Helper()
	return pipeline.Chain(governanceStages(identity, ledger)...,
	)(NewInference(protocol.FormatOpenAIChat, testRouter(t, backendURL), singleAttemptRelay()))
}

// completionRequest fires one POST through the chain and returns the
// status, body and Retry-After header.
func completionRequest(t *testing.T, handler http.Handler, key, body string) (int, string, string) {
	t.Helper()
	rec := post(t, handler, "/v1/chat/completions", key, body)
	return rec.Code, rec.Body.String(), rec.Header().Get("Retry-After")
}

const okBody = `{"model":"m1","messages":[{"role":"user","content":"hello"}]}`

// countingBackend wraps the shared test upstream and counts the requests
// that actually reach it — the assertion surface that a rejected
// (a rejected request must not touch any upstream).
func countingBackend(t *testing.T) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	hits := &atomic.Int64{}
	inner := testUpstreamHandler(t)
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		inner.ServeHTTP(w, r)
	}))
	t.Cleanup(backend.Close)
	return backend, hits
}

func TestChainAuthenticatesAndForwards(t *testing.T) {
	t.Parallel()
	backend := testUpstreamBackend(t)
	defer backend.Close()
	ledger := quota.NewMemory()
	seedBalance(t, ledger, "t1", 1_000_000)
	seedBalance(t, ledger, "t2", 1_000_000)
	handler := buildChain(t, backend.URL, ledger)

	status, body, _ := completionRequest(t, handler, keyT1, okBody)
	if status != http.StatusOK || !strings.Contains(body, `"content":"hi"`) {
		t.Fatalf("status = %d body = %s", status, body)
	}

	// Settlement ran: the usage (3 tokens) left the balance.
	bal, err := ledger.Balance(context.Background(), "t1")
	if err != nil || bal != 1_000_000-3 {
		t.Fatalf("balance = %d err = %v, want 999997 (settled by usage)", bal, err)
	}
}

// TestChainEchoesRequestID pins the correlation contract: the client's
// X-Request-Id is echoed verbatim; an absent one is minted (req- prefix).
func TestChainEchoesRequestID(t *testing.T) {
	t.Parallel()
	backend := testUpstreamBackend(t)
	defer backend.Close()
	handler := buildChain(t, backend.URL, seededLedger(t, "t1", 1_000_000))

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"m1","messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("Authorization", "Bearer "+keyT1)
	req.Header.Set("X-Request-Id", "my-trace-77")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if got := rec.Header().Get("X-Request-Id"); got != "my-trace-77" {
		t.Fatalf("echoed id = %q, want the client value", got)
	}

	req = httptest.NewRequest(http.MethodPost, "/v1/chat/completions",
		strings.NewReader(`{"model":"m1","messages":[{"role":"user","content":"hi"}]}`))
	req.Header.Set("Authorization", "Bearer "+keyT1)
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if got := rec.Header().Get("X-Request-Id"); !strings.HasPrefix(got, "req-") {
		t.Fatalf("minted id = %q, want the req- prefix", got)
	}
}

func TestChainRejectsMissingAndUnknownKeys(t *testing.T) {
	t.Parallel()
	backend, hits := countingBackend(t)
	handler := buildChain(t, backend.URL, quota.NewMemory())

	status, body, _ := completionRequest(t, handler, "", okBody)
	if status != http.StatusUnauthorized || !strings.Contains(body, "missing_api_key") {
		t.Fatalf("no-key status = %d body = %s", status, body)
	}
	status, body, _ = completionRequest(t, handler, "wrong-key", okBody)
	if status != http.StatusUnauthorized || !strings.Contains(body, "invalid_api_key") {
		t.Fatalf("bad-key status = %d body = %s", status, body)
	}
	// An auth rejection never reaches the upstream.
	if got := hits.Load(); got != 0 {
		t.Fatalf("upstream hits = %d, want 0 for rejected keys", got)
	}
}

// TestChainRateLimitsWithRetryAfter covers the over-limit path: over-limit
// requests get 429 with Retry-After and never reach the upstream.
func TestChainRateLimitsWithRetryAfter(t *testing.T) {
	t.Parallel()
	backend, hits := countingBackend(t)
	handler := buildChain(t, backend.URL, seededLedger(t, "t1", 1_000_000))

	// Tier RPM = 2: the first two pass, everything after is rejected.
	for i := 0; i < 2; i++ {
		status, _, _ := completionRequest(t, handler, keyT1, okBody)
		if status != http.StatusOK {
			t.Fatalf("request %d status = %d, want 200", i, status)
		}
	}
	status, body, retryAfter := completionRequest(t, handler, keyT1, okBody)
	if status != http.StatusTooManyRequests {
		t.Fatalf("third request status = %d, want 429", status)
	}
	if retryAfter == "" {
		t.Fatal("429 without Retry-After header")
	}
	if !strings.Contains(body, "rate_limit_exceeded") {
		t.Fatalf("body = %s, want rate limit envelope", body)
	}
	// Exactly the two allowed requests touched the upstream; the
	// 429 stampede behind them never did.
	if got := hits.Load(); got != 2 {
		t.Fatalf("upstream hits = %d, want 2: rejected requests must not reach the upstream", got)
	}
}

// TestChainFailedRequestSettlesAtZero is its failure companion: an upstream
// failure serves no tokens, so the reservation must refund in full —
// a tenant never pays the estimate for a request no upstream answered.
func TestChainFailedRequestSettlesAtZero(t *testing.T) {
	t.Parallel()
	failing := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":{"message":"boom"}}`))
	}))
	defer failing.Close()
	ledger := seededLedger(t, "t1", 1_000_000)
	handler := buildChain(t, failing.URL, ledger)

	status, _, _ := completionRequest(t, handler, keyT1, okBody)
	if status != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500 passthrough", status)
	}
	bal, err := ledger.Balance(context.Background(), "t1")
	if err != nil || bal != 1_000_000 {
		t.Fatalf("balance = %d err = %v, want untouched 1_000_000 (failed request consumed nothing)", bal, err)
	}
}

// TestChainQuotaExhaustionIsPaymentRequired covers a drained
// tenant gets 402 and the upstream stays untouched.
func TestChainQuotaExhaustionIsPaymentRequired(t *testing.T) {
	t.Parallel()
	backend, hits := countingBackend(t)
	// The estimate of okBody (1 prompt word + clamped 50) is 51 tokens;
	// ten cannot cover it.
	ledger := seededLedger(t, "t2", 10)
	handler := buildChain(t, backend.URL, ledger)

	status, body, _ := completionRequest(t, handler, keyT2, okBody)
	if status != http.StatusPaymentRequired {
		t.Fatalf("status = %d, want 402", status)
	}
	if !strings.Contains(body, "insufficient_quota") {
		t.Fatalf("body = %s, want quota envelope", body)
	}
	// Nothing leaked: the rejected reservation is cancelled in full.
	bal, err := ledger.Balance(context.Background(), "t2")
	if err != nil || bal != 10 {
		t.Fatalf("balance = %d err = %v, want untouched 10", bal, err)
	}
	// The 402 never reached the upstream.
	if got := hits.Load(); got != 0 {
		t.Fatalf("upstream hits = %d, want 0 for a quota-rejected request", got)
	}
}

// TestChainStreamedThroughGovernance verifies SSE still arrives chunk
// by chunk after every pipeline layer (the Flusher-survival regression).
func TestChainStreamedThroughGovernance(t *testing.T) {
	t.Parallel()
	backend := testUpstreamBackend(t)
	defer backend.Close()

	identity := tenantIdentity(t, 100, 1_000_000, []string{"*"}, "t1", keyT1)
	// The flusher-survival assembly: governance without the model
	// authorization stage, as this deployment mode mounts it.
	handler := pipeline.Chain(
		pipeline.CarrierStage(),
		pipeline.RequestIDStage(),
		pipeline.FormatStage(protocol.FormatOpenAIChat),
		pipeline.AuthStage(identity),
		limiter.Middleware(limiter.NewMemory(), nil),
		quota.Middleware(seededLedger(t, "t1", 1_000_000), nil),
	)(NewInference(protocol.FormatOpenAIChat, testRouter(t, backend.URL), singleAttemptRelay()))

	resp := openStream(t, handler, streamBody)
	defer func() { _ = resp.Body.Close() }()

	raw := readSSE(t, resp)
	if !strings.Contains(raw, `"content":"hi"`) || !strings.HasSuffix(raw, "data: [DONE]\n\n") {
		t.Fatalf("stream = %q", raw)
	}
}

// streamBody is the streaming completion the SSE tests post.
const streamBody = `{"model":"m1","stream":true,"messages":[{"role":"user","content":"hello"}]}`

// openStream POSTs a streaming completion through handler served on a
// live HTTP server and returns the response; the caller owns the body.
func openStream(t *testing.T, handler http.Handler, body string) *http.Response {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/chat/completions",
		strings.NewReader(body))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+keyT1)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	return resp
}

// readSSE asserts the response streams as text/event-stream and drains
// the body.
func readSSE(t *testing.T, resp *http.Response) string {
	t.Helper()
	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content type = %q", ct)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	return string(raw)
}

// TestChainDeniesModelOutsideTier is the tier-authorization evidence:
// a model the tier does not list is rejected with 403 before any
// routing or upstream contact, whatever the router could serve.
func TestChainDeniesModelOutsideTier(t *testing.T) {
	t.Parallel()
	backend, hits := countingBackend(t)
	ledger := seededLedger(t, "t1", 1_000_000)

	// Explicit list without the wildcard: only m1 may be called, and
	// an empty list would allow nothing (fail-closed).
	identity := tenantIdentity(t, 10, 10_000, []string{"m1"}, "t1", keyT1)
	handler := buildChainWithIdentity(t, backend.URL, ledger, identity)

	status, body, _ := completionRequest(t, handler, keyT1, okBody)
	if status != http.StatusOK {
		t.Fatalf("allowed model status = %d body = %s", status, body)
	}
	status, body, _ = completionRequest(t, handler, keyT1,
		`{"model":"other","messages":[{"role":"user","content":"hello"}]}`)
	if status != http.StatusForbidden || !strings.Contains(body, "model_not_allowed") {
		t.Fatalf("denied model status = %d body = %s, want 403 envelope", status, body)
	}
	// The rejection refunds in full: no quota moved for the denied call,
	// and the denied request never reached the upstream.
	bal, err := ledger.Balance(context.Background(), "t1")
	if err != nil || bal != 1_000_000-3 {
		t.Fatalf("balance = %d err = %v, want 999997 (denied call refunded)", bal, err)
	}
	if got := hits.Load(); got != 1 {
		t.Fatalf("upstream hits = %d, want 1 (only the allowed model): a denied model must not reach the upstream", got)
	}
}

// TestChainClientDisconnectCancelsUpstreamAndSettlesByUsage is the
// settlement evidence: a client that walks away mid-stream has its disconnect
// propagated as upstream cancellation, and the lease settles by the
// tokens actually consumed — never refunded as if nothing happened,
// never surcharged past the reservation.
func TestChainClientDisconnectCancelsUpstreamAndSettlesByUsage(t *testing.T) {
	t.Parallel()
	const initial = int64(1_000_000)
	// The reservation for okBody-like stream requests: 1 prompt word +
	// the tier's 50-token clamp (see buildChain's tier).
	const reservation = int64(51)

	sawCancel := make(chan struct{}, 1)
	backend := endlessStreamBackend(t, sawCancel)
	ledger := &settleWitness{Ledger: seededLedger(t, "t1", initial)}
	handler := buildChain(t, backend.URL, ledger)

	// Read the head of the stream, then walk away mid-flight.
	resp := openStream(t, handler, streamBody)
	if _, err := io.ReadFull(resp.Body, make([]byte, 64)); err != nil {
		t.Fatalf("read stream head: %v", err)
	}
	_ = resp.Body.Close()

	awaitUpstreamCancel(t, sawCancel)
	awaitSettlement(t, ledger, "t1", initial, reservation)
	// The balance move alone cannot distinguish a usage settlement from
	// an abandoned open lease (the reservation already deducted it), so
	// the Settle call itself is the evidence this test exists to pin.
	if ledger.settles.Load() == 0 {
		t.Fatal("no Settle call succeeded: the lease never settled by usage")
	}
}

// endlessStreamBackend serves an SSE stream that never ends on its own;
// it reports on sawCancel when the exchange observes the client (or the
// write side) going away.
func endlessStreamBackend(t *testing.T, sawCancel chan struct{}) *httptest.Server {
	t.Helper()
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher := w.(http.Flusher)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		for i := 0; ; i++ {
			if r.Context().Err() != nil {
				sawCancel <- struct{}{}
				return
			}
			// nosemgrep: go.lang.security.audit.xss.no-fprintf-to-responsewriter.no-fprintf-to-responsewriter -- test fixture writes an opaque SSE frame; no HTML is rendered
			_, err := fmt.Fprintf(w, "data: {\"choices\":[{\"delta\":{\"content\":\"chunk %d \"}}]}\n\n", i)
			if err != nil {
				sawCancel <- struct{}{}
				return
			}
			flusher.Flush()
			time.Sleep(10 * time.Millisecond)
		}
	}))
	t.Cleanup(backend.Close)
	return backend
}

// awaitUpstreamCancel fails the test unless the upstream exchange
// observes the client disconnect within the wait window.
func awaitUpstreamCancel(t *testing.T, sawCancel <-chan struct{}) {
	t.Helper()
	select {
	case <-sawCancel:
	case <-time.After(5 * time.Second):
		t.Fatal("upstream exchange never observed the client disconnect")
	}
}

// settleWitness wraps a ledger and counts the Settle calls that ended
// successfully, so a test can prove settlement ran instead of inferring
// it from a balance the reservation alone already explains.
type settleWitness struct {
	quota.Ledger
	settles atomic.Int64
}

func (w *settleWitness) Settle(ctx context.Context, leaseID string, usedTokens int64) error {
	err := w.Ledger.Settle(ctx, leaseID, usedTokens)
	if err == nil {
		w.settles.Add(1)
	}
	return err
}

// awaitSettlement polls until the lease settles below the initial
// balance: anything below proves tokens were consumed, the floor proves
// the disconnected request was never surcharged past the reservation.
// The balance alone cannot prove settlement — the reservation itself
// already deducts it — so pair this helper with the witness's count.
func awaitSettlement(t *testing.T, ledger quota.Ledger, tenantID string, initial, reservation int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		bal, err := ledger.Balance(context.Background(), tenantID)
		if err != nil {
			t.Fatalf("balance: %v", err)
		}
		if bal < initial {
			if floor := initial - reservation; bal < floor {
				t.Fatalf("balance = %d, below the reserved worst case %d: the settle surcharged a disconnected request", bal, floor)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("balance = %d still full after the disconnect: the lease never settled by usage", bal)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
