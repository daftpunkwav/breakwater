/**
 * @file chain_test
 * @description The composed governance pipeline end to end: identity,
 * rate limiting and quota around the completions route — the S2 and S5
 * acceptance scenarios plus the auth rejections.
 */
package server

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/daftpunkwav/breakwater/internal/auth"
	"github.com/daftpunkwav/breakwater/internal/limiter"
	"github.com/daftpunkwav/breakwater/internal/pipeline"
	"github.com/daftpunkwav/breakwater/internal/quota"
	"github.com/daftpunkwav/breakwater/internal/relay"
	"github.com/daftpunkwav/breakwater/internal/retry"
	"github.com/daftpunkwav/breakwater/internal/router"
	"github.com/daftpunkwav/breakwater/internal/upstream"
)

const (
	keyT1 = "key-t1"
	keyT2 = "key-t2"
)

// buildChain assembles the full stage chain over the completions
// handler with in-memory backends and the shared test upstream.
func buildChain(t *testing.T, backendURL string, ledger quota.Ledger) http.Handler {
	t.Helper()
	identity, err := auth.NewStatic(auth.StaticConfig{
		Tiers: []auth.StaticTier{
			{ID: "free", RPM: 2, TPM: 10_000, MaxTokens: 50, MonthlyQuota: 100_000},
		},
		Tenants: []auth.StaticTenant{
			{ID: "t1", Name: "Tenant One", Tier: "free", Keys: []string{keyT1}},
			{ID: "t2", Name: "Tenant Two", Tier: "free", Keys: []string{keyT2}},
		},
	})
	if err != nil {
		t.Fatalf("identity: %v", err)
	}

	adapter, err := upstream.NewOpenAI(upstream.OpenAIConfig{ID: "test", BaseURL: backendURL})
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	rt, err := router.NewPriority([]router.Binding{{Models: []string{"*"}, Upstream: adapter}})
	if err != nil {
		t.Fatalf("router: %v", err)
	}
	relayer := relay.New(retry.Policy{MaxAttempts: 1}, retry.NewBudget(8))

	stages := []pipeline.Middleware{
		pipeline.CarrierStage(),
		pipeline.AuthStage(identity),
		limiter.Middleware(limiter.NewMemory(), nil),
		quota.Middleware(ledger, nil),
	}
	return pipeline.Chain(stages...)(NewCompletions(rt, relayer))
}

// completionRequest fires one POST through the chain and returns the
// status, body and Retry-After header.
func completionRequest(t *testing.T, handler http.Handler, key, body string) (int, string, string) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec.Code, rec.Body.String(), rec.Header().Get("Retry-After")
}

const okBody = `{"model":"m1","messages":[{"role":"user","content":"hello"}]}`

func TestChainAuthenticatesAndForwards(t *testing.T) {
	t.Parallel()
	backend := testUpstreamBackend(t)
	defer backend.Close()
	ledger := quota.NewMemory()
	ledger.SetBalance("t1", 1_000_000)
	ledger.SetBalance("t2", 1_000_000)
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

func TestChainRejectsMissingAndUnknownKeys(t *testing.T) {
	t.Parallel()
	backend := testUpstreamBackend(t)
	defer backend.Close()
	ledger := quota.NewMemory()
	handler := buildChain(t, backend.URL, ledger)

	status, body, _ := completionRequest(t, handler, "", okBody)
	if status != http.StatusUnauthorized || !strings.Contains(body, "missing_api_key") {
		t.Fatalf("no-key status = %d body = %s", status, body)
	}
	status, body, _ = completionRequest(t, handler, "wrong-key", okBody)
	if status != http.StatusUnauthorized || !strings.Contains(body, "invalid_api_key") {
		t.Fatalf("bad-key status = %d body = %s", status, body)
	}
}

// TestChainRateLimitsWithRetryAfter is scenario S2: over-limit
// requests get 429 with Retry-After and never reach the upstream.
func TestChainRateLimitsWithRetryAfter(t *testing.T) {
	t.Parallel()
	backend := testUpstreamBackend(t)
	defer backend.Close()
	ledger := quota.NewMemory()
	ledger.SetBalance("t1", 1_000_000)
	handler := buildChain(t, backend.URL, ledger)

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
}

// TestChainQuotaExhaustionIsPaymentRequired is scenario S5: a drained
// tenant gets 402 and the upstream stays untouched.
func TestChainQuotaExhaustionIsPaymentRequired(t *testing.T) {
	t.Parallel()
	backend := testUpstreamBackend(t)
	defer backend.Close()
	ledger := quota.NewMemory()
	// The estimate of okBody (1 prompt word + clamped 50) is 51 tokens;
	// ten cannot cover it.
	ledger.SetBalance("t2", 10)
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
}

// TestChainStreamedThroughGovernance verifies SSE still arrives chunk
// by chunk after every pipeline layer (the Flusher-survival regression).
func TestChainStreamedThroughGovernance(t *testing.T) {
	t.Parallel()
	backend := testUpstreamBackend(t)
	defer backend.Close()
	ledger := quota.NewMemory()
	ledger.SetBalance("t1", 1_000_000)

	identity, err := auth.NewStatic(auth.StaticConfig{
		Tiers:   []auth.StaticTier{{ID: "free", RPM: 100, TPM: 1_000_000, MaxTokens: 50, MonthlyQuota: 100_000}},
		Tenants: []auth.StaticTenant{{ID: "t1", Name: "T1", Tier: "free", Keys: []string{keyT1}}},
	})
	if err != nil {
		t.Fatalf("identity: %v", err)
	}
	adapter, err := upstream.NewOpenAI(upstream.OpenAIConfig{ID: "test", BaseURL: backend.URL})
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	rt, err := router.NewPriority([]router.Binding{{Models: []string{"*"}, Upstream: adapter}})
	if err != nil {
		t.Fatalf("router: %v", err)
	}
	relayer := relay.New(retry.Policy{MaxAttempts: 1}, retry.NewBudget(8))
	handler := pipeline.Chain(
		pipeline.CarrierStage(),
		pipeline.AuthStage(identity),
		limiter.Middleware(limiter.NewMemory(), nil),
		quota.Middleware(ledger, nil),
	)(NewCompletions(rt, relayer))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/chat/completions",
		strings.NewReader(`{"model":"m1","stream":true,"messages":[{"role":"user","content":"hello"}]}`))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	req.Header.Set("Authorization", "Bearer "+keyT1)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Fatalf("content type = %q", ct)
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if !strings.Contains(string(raw), `"content":"hi"`) || !strings.HasSuffix(string(raw), "data: [DONE]\n\n") {
		t.Fatalf("stream = %q", raw)
	}
}
