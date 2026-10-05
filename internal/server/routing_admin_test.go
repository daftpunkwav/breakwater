/**
 * @file routing_admin_test
 * @description The runtime routing switches end to end: the admin
 * endpoints (view, model switch, upstream switch, unknown-name and
 * payload rejections) and the 403 an inference request receives for a
 * model an operator has disabled.
 */
package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/daftpunkwav/breakwater/internal/pipeline"
	"github.com/daftpunkwav/breakwater/internal/protocol"
	"github.com/daftpunkwav/breakwater/internal/relay"
	"github.com/daftpunkwav/breakwater/internal/retry"
	"github.com/daftpunkwav/breakwater/internal/router"
	"github.com/daftpunkwav/breakwater/internal/upstream"
)

// switchAdmin builds the admin surface wired to a fresh routing switch
// over the m1 model and the u1 upstream.
func switchAdmin() (*router.Switch, *Admin) {
	sw := router.NewSwitch([]string{"m1"}, []string{"u1"})
	return sw, NewAdmin("", nil, nil, nil, WithRouting(sw))
}

// adminRequest fires one request at the admin endpoint and returns the
// recorder.
func adminRequest(admin *Admin, method, target, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	rec := httptest.NewRecorder()
	admin.ServeHTTP(rec, req)
	return rec
}

func TestAdminRoutingView(t *testing.T) {
	t.Parallel()
	_, admin := switchAdmin()

	rec := adminRequest(admin, http.MethodGet, "/admin/routing", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var body router.SwitchView
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body = %s err = %v", rec.Body.String(), err)
	}
	if !body.Models["m1"] || !body.Upstreams["u1"] {
		t.Fatalf("view = %+v, want every known name enabled", body)
	}

	// Without an installed switch the endpoint closes.
	bare := NewAdmin("", nil, nil, nil)
	if rec := adminRequest(bare, http.MethodGet, "/admin/routing", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("bare switch status = %d, want 404", rec.Code)
	}
}

func TestAdminModelSwitchEndpoint(t *testing.T) {
	t.Parallel()
	sw, admin := switchAdmin()
	put := func(model, body string) *httptest.ResponseRecorder {
		return adminRequest(admin, http.MethodPut, "/admin/models/"+model, body)
	}

	// A typo'd extra member must fail the write loudly — never a silent
	// no-op on a governance switch.
	if rec := put("m1", `{"enabled":true,"disabld":false}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("unknown member status = %d, want 400", rec.Code)
	}
	if !sw.ModelEnabled("m1") {
		t.Fatal("a rejected payload toggled the switch")
	}

	if rec := put("m1", `{"enabled":false}`); rec.Code != http.StatusOK {
		t.Fatalf("disable status = %d body = %s", rec.Code, rec.Body.String())
	}
	if sw.ModelEnabled("m1") {
		t.Fatal("disable did not reach the switch")
	}

	// A non-PUT method is rejected like every other admin write.
	if rec := adminRequest(admin, http.MethodGet, "/admin/models/m1", ""); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET status = %d, want 405", rec.Code)
	}

	if rec := put("typo", `{"enabled":false}`); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown model status = %d, want 404", rec.Code)
	}
	if rec := put("m1", `{"enabled":"yes"}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad payload status = %d, want 400", rec.Code)
	}
	// An empty id names nothing — the same 404, not a silent toggle.
	if rec := put("", `{"enabled":false}`); rec.Code != http.StatusNotFound {
		t.Fatalf("empty model id status = %d, want 404", rec.Code)
	}

	// Without an installed switch the endpoints close.
	bare := NewAdmin("", nil, nil, nil)
	for _, path := range []string{"/admin/models/m1", "/admin/upstreams/u1"} {
		if rec := adminRequest(bare, http.MethodPut, path, `{"enabled":false}`); rec.Code != http.StatusNotFound {
			t.Fatalf("%s without a switch = %d, want 404", path, rec.Code)
		}
	}
}

func TestAdminUpstreamSwitchEndpoint(t *testing.T) {
	t.Parallel()
	sw, admin := switchAdmin()
	put := func(path, body string) *httptest.ResponseRecorder {
		return adminRequest(admin, http.MethodPut, path, body)
	}

	if rec := put("/admin/upstreams/u1", `{"enabled":false}`); rec.Code != http.StatusOK || sw.UpstreamEnabled("u1") {
		t.Fatalf("status = %d body = %s, want the upstream disabled", rec.Code, rec.Body.String())
	}

	if rec := put("/admin/upstreams/typo", `{"enabled":false}`); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown upstream status = %d, want 404", rec.Code)
	}

	// A malformed switch payload is a 400 and never touches the switch.
	if rec := put("/admin/upstreams/u1", `not json`); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad payload status = %d, want 400", rec.Code)
	}
	if sw.UpstreamEnabled("u1") {
		t.Fatal("a rejected payload toggled the switch")
	}

	if rec := put("/admin/upstreams/", `{"enabled":false}`); rec.Code != http.StatusNotFound {
		t.Fatalf("empty upstream id status = %d, want 404", rec.Code)
	}
}

// TestDisabledModelRequestIs403: after the operator switch, an
// inference request for the model is refused with model_disabled —
// distinct from 404 (never configured) and 503 (unhealthy).
func TestDisabledModelRequestIs403(t *testing.T) {
	t.Parallel()
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer backend.Close()

	adapter, err := upstream.NewOpenAI(upstream.OpenAIConfig{ID: "test", BaseURL: backend.URL})
	if err != nil {
		t.Fatalf("build adapter: %v", err)
	}
	sw := router.NewSwitch([]string{"m1"}, []string{"test"})
	priority, err := router.NewPriority([]router.Binding{{Models: []string{"m1"}, Upstream: adapter}}, router.WithSwitch(sw))
	if err != nil {
		t.Fatalf("build router: %v", err)
	}
	relayer := relay.New(retry.Policy{MaxAttempts: 1}, retry.NewBudget(2))
	handler := pipeline.Chain(
		pipeline.CarrierStage(),
		pipeline.FormatStage(protocol.FormatOpenAIChat),
	)(NewInference(protocol.FormatOpenAIChat, priority, relayer))

	if err := sw.SetModel("m1", false); err != nil {
		t.Fatalf("disable model: %v", err)
	}

	srv := httptest.NewServer(handler)
	defer srv.Close()
	resp, err := http.Post(srv.URL+"/v1/chat/completions", "application/json",
		strings.NewReader(`{"model":"m1","messages":[{"role":"user","content":"hi"}]}`))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw := make([]byte, 512)
	n, _ := resp.Body.Read(raw)

	if resp.StatusCode != http.StatusForbidden || !strings.Contains(string(raw[:n]), `"code":"model_disabled"`) {
		t.Fatalf("status = %d body = %s, want 403 model_disabled envelope", resp.StatusCode, raw[:n])
	}
}
