/**
 * @file model_authz_cache_test
 * @description The cache/authorization interplay: a tier-denied model
 * must be refused even when the exact request body already sits in the
 * cache from an allowed tenant — the cache key is the request body
 * alone, so the model authorization stage has to run before the cache
 * stage is ever consulted.
 */
package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/daftpunkwav/breakwater/internal/auth"
	"github.com/daftpunkwav/breakwater/internal/cache"
	"github.com/daftpunkwav/breakwater/internal/limiter"
	"github.com/daftpunkwav/breakwater/internal/obs"
	"github.com/daftpunkwav/breakwater/internal/pipeline"
	"github.com/daftpunkwav/breakwater/internal/protocol"
	"github.com/daftpunkwav/breakwater/internal/quota"
	"github.com/daftpunkwav/breakwater/internal/relay"
	"github.com/daftpunkwav/breakwater/internal/retry"
	"github.com/daftpunkwav/breakwater/internal/router"
	"github.com/daftpunkwav/breakwater/internal/upstream"
)

func TestCacheHitCannotBypassTierModelAuthorization(t *testing.T) {
	t.Parallel()
	hits := 0
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"hi"}}],` +
			`"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`))
	}))
	defer backend.Close()

	identity, err := auth.NewStatic(auth.StaticConfig{
		Tiers: []auth.StaticTier{
			{ID: "wide", RPM: 10, TPM: 10_000, MaxTokens: 50, MonthlyQuota: 100_000, AllowedModels: []string{"m1", "m2"}},
			{ID: "narrow", RPM: 10, TPM: 10_000, MaxTokens: 50, MonthlyQuota: 100_000, AllowedModels: []string{"m1"}},
		},
		Tenants: []auth.StaticTenant{
			{ID: "tw", Name: "Wide", Tier: "wide", Keys: []string{keyT1}},
			{ID: "tn", Name: "Narrow", Tier: "narrow", Keys: []string{keyT2}},
		},
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
	ledger := quota.NewMemory()
	if err := ledger.SetBalance(context.Background(), "tw", 1_000_000); err != nil {
		t.Fatalf("seed tw: %v", err)
	}
	if err := ledger.SetBalance(context.Background(), "tn", 1_000_000); err != nil {
		t.Fatalf("seed tn: %v", err)
	}

	handler := pipeline.Chain(
		pipeline.CarrierStage(),
		pipeline.RequestIDStage(),
		pipeline.FormatStage(protocol.FormatOpenAIChat),
		pipeline.AuthStage(identity),
		pipeline.ModelAuthzStage(),
		limiter.Middleware(limiter.NewMemory(), nil),
		quota.Middleware(ledger, nil),
		cache.Middleware(cache.NewMemory(), cache.NewFlight(), time.Minute, obs.NewMetrics()),
	)(NewInference(protocol.FormatOpenAIChat, rt, relay.New(retry.Policy{MaxAttempts: 1}, retry.NewBudget(8))))

	// Cache-eligible body: explicitly deterministic parameters.
	const body = `{"model":"m2","temperature":0,"messages":[{"role":"user","content":"hello"}]}`

	// The wide tenant may call m2; the exchange populates the cache.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+keyT1)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("wide tenant status = %d body = %s, want 200", rec.Code, rec.Body.String())
	}
	if hits != 1 {
		t.Fatalf("upstream hits = %d, want 1 after the wide tenant's fetch", hits)
	}

	// The narrow tenant sends the byte-identical body: a cache hit by
	// key, but its tier denies m2. The authorization stage must refuse
	// before the cache stage — a replay may not launder the model past
	// the tier's allow list.
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+keyT2)
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden || !strings.Contains(rec.Body.String(), "model_not_allowed") {
		t.Fatalf("narrow tenant status = %d body = %s, want 403 model_not_allowed", rec.Code, rec.Body.String())
	}
	if hits != 1 {
		t.Fatalf("upstream hits = %d, want 1 (the denied request touched nothing)", hits)
	}
}
