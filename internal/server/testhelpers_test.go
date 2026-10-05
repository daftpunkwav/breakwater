/**
 * @file testhelpers_test
 * @description Shared fixtures for the server package's end-to-end
 * tests: the single-binding router and single-attempt relay every
 * governed chain mounts, the static identity store, the governance
 * stage order, quota seeding, and the recorder round-trip.
 */
package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/daftpunkwav/breakwater/internal/auth"
	"github.com/daftpunkwav/breakwater/internal/limiter"
	"github.com/daftpunkwav/breakwater/internal/pipeline"
	"github.com/daftpunkwav/breakwater/internal/protocol"
	"github.com/daftpunkwav/breakwater/internal/quota"
	"github.com/daftpunkwav/breakwater/internal/relay"
	"github.com/daftpunkwav/breakwater/internal/retry"
	"github.com/daftpunkwav/breakwater/internal/router"
	"github.com/daftpunkwav/breakwater/internal/upstream"
)

// testRouter builds the single-binding priority router over backendURL:
// every model routes to one OpenAI-compatible test upstream.
func testRouter(t *testing.T, backendURL string) *router.Priority {
	t.Helper()
	adapter, err := upstream.NewOpenAI(upstream.OpenAIConfig{ID: "test", BaseURL: backendURL})
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	priority, err := router.NewPriority([]router.Binding{{Models: []string{"*"}, Upstream: adapter}})
	if err != nil {
		t.Fatalf("router: %v", err)
	}
	return priority
}

// singleAttemptRelay is the relay the pipeline tests mount: exactly one
// upstream attempt over the shared small retry budget.
func singleAttemptRelay() *relay.Executor {
	return relay.New(retry.Policy{MaxAttempts: 1}, retry.NewBudget(8))
}

// mustIdentity builds the static identity store or fails the test.
func mustIdentity(t *testing.T, cfg auth.StaticConfig) auth.Store {
	t.Helper()
	identity, err := auth.NewStatic(cfg)
	if err != nil {
		t.Fatalf("identity: %v", err)
	}
	return identity
}

// tenantIdentity builds the single-tenant free-tier store the
// governance tests mount: the tier's request rate, token rate and
// model allow-list are the knobs they vary. The tenant carries one key
// and is named by its upper-cased id.
func tenantIdentity(t *testing.T, rpm, tpm int64, allowedModels []string, tenantID, key string) auth.Store {
	t.Helper()
	return mustIdentity(t, auth.StaticConfig{
		Tiers: []auth.StaticTier{
			{ID: "free", RPM: rpm, TPM: tpm, MaxTokens: 50, MonthlyQuota: 100_000, AllowedModels: allowedModels},
		},
		Tenants: []auth.StaticTenant{{ID: tenantID, Name: strings.ToUpper(tenantID), Tier: "free", Keys: []string{key}}},
	})
}

// governanceStages is the stage order the governed end-to-end tests
// mount in front of inference: identity, model authorization, rate
// limit, quota. extra appends the caller's tail stages (the cache).
func governanceStages(identity auth.Store, ledger quota.Ledger, extra ...pipeline.Middleware) []pipeline.Middleware {
	stages := []pipeline.Middleware{
		pipeline.CarrierStage(),
		pipeline.RequestIDStage(),
		pipeline.FormatStage(protocol.FormatOpenAIChat),
		pipeline.AuthStage(identity),
		pipeline.ModelAuthzStage(),
		limiter.Middleware(limiter.NewMemory(), nil),
		quota.Middleware(ledger, nil),
	}
	return append(stages, extra...)
}

// seedBalance tops the tenant's ledger balance or fails the test.
func seedBalance(t *testing.T, ledger quota.Ledger, tenantID string, tokens int64) {
	t.Helper()
	if err := ledger.SetBalance(context.Background(), tenantID, tokens); err != nil {
		t.Fatalf("seed %s: %v", tenantID, err)
	}
}

// seededLedger is a fresh in-memory ledger with the tenant's balance
// topped to tokens.
func seededLedger(t *testing.T, tenantID string, tokens int64) quota.Ledger {
	t.Helper()
	ledger := quota.NewMemory()
	seedBalance(t, ledger, tenantID, tokens)
	return ledger
}

// post fires one POST through the handler and returns the recorder.
func post(t *testing.T, handler http.Handler, path, key, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec
}
