/**
 * @file authzstage_test
 * @description The tier model authorization stage: the 403 for a model
 * outside the tier, the pass-through for an allowed one, the
 * model-presence rule, the malformed-body rejection and the ungoverned
 * (zero tenant) bypass — every decision before the wrapped handler.
 */
package pipeline

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/daftpunkwav/breakwater/internal/auth"
	"github.com/daftpunkwav/breakwater/internal/protocol"
)

// authzSpy is the wrapped handler: it reports whether the stage let the
// request through.
type authzSpy struct {
	called bool
}

func (s *authzSpy) ServeHTTP(_ http.ResponseWriter, _ *http.Request) { s.called = true }

// authzFire runs one request through the stage and returns the recorder
// and the wrapped handler state.
func authzFire(t *testing.T, carrier *Carrier, body string) (*httptest.ResponseRecorder, *authzSpy) {
	t.Helper()
	spy := &authzSpy{}
	handler := ModelAuthzStage()(spy)
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	req = req.WithContext(WithCarrier(req.Context(), carrier))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec, spy
}

func TestAuthzStageDeniesModelOutsideTier(t *testing.T) {
	t.Parallel()
	carrier := &Carrier{
		Format: protocol.FormatOpenAIChat,
		Tenant: auth.Tenant{ID: "t1", Tier: auth.Tier{AllowedModels: []string{"m1"}}},
	}
	rec, spy := authzFire(t, carrier, `{"model":"m2","messages":[{"role":"user","content":"hi"}]}`)
	if rec.Code != http.StatusForbidden || !strings.Contains(rec.Body.String(), "model_not_allowed") {
		t.Fatalf("status = %d body = %s, want 403 model_not_allowed", rec.Code, rec.Body.String())
	}
	if spy.called {
		t.Fatal("the denied request reached the wrapped handler")
	}
}

func TestAuthzStageDenyBeatsAllowAndWildcard(t *testing.T) {
	t.Parallel()
	carrier := &Carrier{
		Format: protocol.FormatOpenAIChat,
		Tenant: auth.Tenant{ID: "t1", Tier: auth.Tier{
			AllowedModels: []string{"*"},
			DeniedModels:  []string{"m2"},
		}},
	}
	rec, spy := authzFire(t, carrier, `{"model":"m2","messages":[{"role":"user","content":"hi"}]}`)
	if rec.Code != http.StatusForbidden || spy.called {
		t.Fatalf("status = %d called = %v, want the deny list to win", rec.Code, spy.called)
	}
}

func TestAuthzStageAllowsListModel(t *testing.T) {
	t.Parallel()
	carrier := &Carrier{
		Format: protocol.FormatOpenAIChat,
		Tenant: auth.Tenant{ID: "t1", Tier: auth.Tier{AllowedModels: []string{"m1"}}},
	}
	rec, spy := authzFire(t, carrier, `{"model":"m1","messages":[{"role":"user","content":"hi"}]}`)
	if rec.Code != http.StatusOK || !spy.called {
		t.Fatalf("status = %d called = %v, want the allowed model through", rec.Code, spy.called)
	}
}

// TestAuthzStageUngovernedBypass: without an identity store the carrier
// carries a zero tenant — the stage must not trip fail-closed on it.
// The inference handler owns the same check for that mode.
func TestAuthzStageUngovernedBypass(t *testing.T) {
	t.Parallel()
	carrier := &Carrier{Format: protocol.FormatOpenAIChat}
	rec, spy := authzFire(t, carrier, `{"model":"anything","messages":[{"role":"user","content":"hi"}]}`)
	if rec.Code != http.StatusOK || !spy.called {
		t.Fatalf("status = %d called = %v, want the zero tenant through", rec.Code, spy.called)
	}
}

func TestAuthzStageRequiresModel(t *testing.T) {
	t.Parallel()
	carrier := &Carrier{Format: protocol.FormatOpenAIChat,
		Tenant: auth.Tenant{ID: "t1", Tier: auth.Tier{AllowedModels: []string{"*"}}}}
	rec, spy := authzFire(t, carrier, `{"messages":[{"role":"user","content":"hi"}]}`)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "model is required") {
		t.Fatalf("status = %d body = %s, want 400 model is required", rec.Code, rec.Body.String())
	}
	if spy.called {
		t.Fatal("a model-less request reached the wrapped handler")
	}
}

func TestAuthzStageRejectsMalformedBody(t *testing.T) {
	t.Parallel()
	carrier := &Carrier{Format: protocol.FormatOpenAIChat,
		Tenant: auth.Tenant{ID: "t1", Tier: auth.Tier{AllowedModels: []string{"*"}}}}
	rec, spy := authzFire(t, carrier, `{malformed`)
	if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), "invalid_request") {
		t.Fatalf("status = %d body = %s, want 400 invalid_request", rec.Code, rec.Body.String())
	}
	if spy.called {
		t.Fatal("a malformed body reached the wrapped handler")
	}
}

// TestAuthzStageRendersInClientFormat pins the format discipline: the
// rejection is written in the carrier's wire, here anthropic-messages
// (whose error vocabulary folds the code into its generic api_error).
func TestAuthzStageRendersInClientFormat(t *testing.T) {
	t.Parallel()
	carrier := &Carrier{
		Format: protocol.FormatAnthropicMessages,
		Tenant: auth.Tenant{ID: "t1", Tier: auth.Tier{AllowedModels: []string{"m1"}}},
	}
	rec, spy := authzFire(t, carrier, `{"model":"m2","max_tokens":1,"messages":[]}`)
	if rec.Code != http.StatusForbidden || !strings.Contains(rec.Body.String(), "api_error") {
		t.Fatalf("status = %d body = %s, want the anthropic-format 403", rec.Code, rec.Body.String())
	}
	if spy.called {
		t.Fatal("the denied request reached the wrapped handler")
	}
}
