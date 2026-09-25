/**
 * @file auth_stage_test
 * @description The authentication stage: key extraction from both
 * header shapes, the store outcome mapping (401/503/attach), the
 * missing-carrier guard and the client-format error rendering.
 */
package pipeline

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/daftpunkwav/breakwater/internal/auth"
	"github.com/daftpunkwav/breakwater/internal/protocol"
)

// TestAPIKeyOfExtraction covers the accepted Authorization shape and
// the x-api-key fallback, plus every rejection branch.
func TestAPIKeyOfExtraction(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		authHeader  string
		apiKeyHdr   string
		wantKey     string
		wantPresent bool
	}{
		{"bearer key", "Bearer sk-1", "", "sk-1", true},
		{"bearer with padded key", "Bearer   sk-1  ", "", "sk-1", true},
		{"lowercase scheme falls through", "bearer sk-1", "", "", false},
		{"other scheme falls through", "Basic dXNlcjpwYXNz", "", "", false},
		{"empty bearer key", "Bearer ", "", "", false},
		{"x-api-key fallback", "", " sk-x ", "sk-x", true},
		{"blank x-api-key", "", "   ", "", false},
		{"no headers at all", "", "", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			if tc.authHeader != "" {
				req.Header.Set("Authorization", tc.authHeader)
			}
			if tc.apiKeyHdr != "" {
				req.Header.Set("x-api-key", tc.apiKeyHdr)
			}

			key, ok := apiKeyOf(req)
			if ok != tc.wantPresent || key != tc.wantKey {
				t.Fatalf("apiKeyOf = (%q, %v), want (%q, %v)", key, ok, tc.wantKey, tc.wantPresent)
			}
		})
	}
}

// serveThroughAuth runs one request through the auth stage with the
// given store and returns the downstream observation and the recorder.
func serveThroughAuth(t *testing.T, store auth.Store, format protocol.Format, headers map[string]string) (*httptest.ResponseRecorder, *Carrier, bool) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	req = req.WithContext(WithCarrier(context.Background(), &Carrier{Format: format}))

	var downstream *Carrier
	served := false
	handler := AuthStage(store)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		served = true
		downstream = CarrierFrom(r.Context())
	}))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	return rec, downstream, served
}

// TestAuthStageAttachesTenantAndForwards: a resolved key rides
// downstream on the carrier.
func TestAuthStageAttachesTenantAndForwards(t *testing.T) {
	t.Parallel()

	store := &stubStore{tenant: auth.Tenant{ID: "tenant-1", Name: "Acme"}}
	rec, carrier, served := serveThroughAuth(t, store, protocol.FormatOpenAIChat,
		map[string]string{"Authorization": "Bearer sk-1"})

	if !served {
		t.Fatal("valid key did not reach the downstream handler")
	}
	if carrier == nil || carrier.Tenant.ID != "tenant-1" || carrier.Tenant.Name != "Acme" {
		t.Fatalf("carrier = %+v, want tenant-1 attached", carrier)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
}

// TestAuthStageAcceptsXAPIKey: Anthropic-style clients authenticate
// through the fallback header.
func TestAuthStageAcceptsXAPIKey(t *testing.T) {
	t.Parallel()

	_, _, served := serveThroughAuth(t, &stubStore{tenant: auth.Tenant{ID: "t"}},
		protocol.FormatOpenAIChat, map[string]string{"x-api-key": "sk-1"})
	if !served {
		t.Fatal("x-api-key did not authenticate")
	}
}

// TestAuthStageRejectsMissingKey: no usable key is a 401 before the
// store is consulted.
func TestAuthStageRejectsMissingKey(t *testing.T) {
	t.Parallel()

	rec, _, served := serveThroughAuth(t, &stubStore{}, protocol.FormatOpenAIChat, nil)
	if served {
		t.Fatal("request without a key reached downstream")
	}
	assertEnvelope(t, rec, http.StatusUnauthorized, "missing_api_key")
}

// TestAuthStageRejectsUnknownKey: the definitive store refusal maps to
// 401 invalid_api_key.
func TestAuthStageRejectsUnknownKey(t *testing.T) {
	t.Parallel()

	rec, _, served := serveThroughAuth(t, &stubStore{err: auth.ErrUnauthorized},
		protocol.FormatOpenAIChat, map[string]string{"Authorization": "Bearer sk-revoked"})
	if served {
		t.Fatal("unauthorized key reached downstream")
	}
	assertEnvelope(t, rec, http.StatusUnauthorized, "invalid_api_key")
}

// TestAuthStageRejectsStoreOutage: a transient store failure is 503,
// never a silent pass.
func TestAuthStageRejectsStoreOutage(t *testing.T) {
	t.Parallel()

	rec, _, served := serveThroughAuth(t, &stubStore{err: errStoreDown},
		protocol.FormatOpenAIChat, map[string]string{"Authorization": "Bearer sk-1"})
	if served {
		t.Fatal("store outage let the request through")
	}
	assertEnvelope(t, rec, http.StatusServiceUnavailable, "identity_unavailable")
}

// TestAuthStageGuardMissingCarrier: a resolved tenant without the
// chain-entry carrier is a misconfiguration, not a pass.
func TestAuthStageGuardMissingCarrier(t *testing.T) {
	t.Parallel()

	req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", nil)
	req.Header.Set("Authorization", "Bearer sk-1")

	served := false
	handler := AuthStage(&stubStore{tenant: auth.Tenant{ID: "t"}})(
		http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { served = true }))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if served {
		t.Fatal("request without a carrier reached downstream")
	}
	assertEnvelope(t, rec, http.StatusInternalServerError, "pipeline_misconfigured")
}

// TestAuthStageRendersAnthropicEnvelope: rejections render in the
// carrier's client format, not always the canonical one.
func TestAuthStageRendersAnthropicEnvelope(t *testing.T) {
	t.Parallel()

	rec, _, _ := serveThroughAuth(t, &stubStore{}, protocol.FormatAnthropicMessages, nil)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401", rec.Code)
	}
	body := rec.Body.String()
	// The Messages envelope carries no code field; the type and the
	// message identify the failure.
	for _, fragment := range []string{`"type":"error"`, "x-api-key header"} {
		if !strings.Contains(body, fragment) {
			t.Fatalf("anthropic envelope missing %s: %s", fragment, body)
		}
	}
}

// TestRenderForFallsBackToCanonical: without a carrier the canonical
// envelope is used instead of an empty format.
func TestRenderForFallsBackToCanonical(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/", nil)

	renderFor(req, rec, http.StatusUnauthorized, "missing_api_key", "no key")

	assertEnvelope(t, rec, http.StatusUnauthorized, "missing_api_key")
}
