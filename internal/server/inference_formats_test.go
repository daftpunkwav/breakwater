/**
 * @file inference_formats_test
 * @description The three client formats end to end: each route walks
 * its own chain, the relay speaks canonical chat upstream, and the
 * client sees its own protocol — buffered, streamed, and on honest
 * stream termination.
 */
package server

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/daftpunkwav/breakwater/internal/auth"
	"github.com/daftpunkwav/breakwater/internal/pipeline"
	"github.com/daftpunkwav/breakwater/internal/protocol"
	"github.com/daftpunkwav/breakwater/internal/relay"
	"github.com/daftpunkwav/breakwater/internal/retry"
	"github.com/daftpunkwav/breakwater/internal/router"
	"github.com/daftpunkwav/breakwater/internal/upstream"
)

// buildAllRoutes mounts the three inference routes over one backend.
func buildAllRoutes(t *testing.T, backendURL string) http.Handler {
	t.Helper()
	adapter, err := upstream.NewOpenAI(upstream.OpenAIConfig{ID: "test", BaseURL: backendURL})
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	priority, err := router.NewPriority([]router.Binding{{Models: []string{"*"}, Upstream: adapter}})
	if err != nil {
		t.Fatalf("router: %v", err)
	}
	relayer := relay.New(retry.Policy{MaxAttempts: 1}, retry.NewBudget(8))

	inference := make(map[protocol.Format]http.Handler, 3)
	for _, format := range []protocol.Format{
		protocol.FormatOpenAIChat, protocol.FormatOpenAIResponses, protocol.FormatAnthropicMessages,
	} {
		handler := pipeline.Chain(
			pipeline.CarrierStage(),
			pipeline.FormatStage(format),
		)(NewInference(format, priority, relayer))
		inference[format] = handler
	}
	return newRootHandler(inference, nil, nil, "test", nil, nil)
}

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

// servedRoutes mounts the three inference routes over one fresh test
// upstream, cleaned up with the test.
func servedRoutes(t *testing.T) http.Handler {
	t.Helper()
	backend := testUpstreamBackend(t)
	t.Cleanup(backend.Close)
	return buildAllRoutes(t, backend.URL)
}

// bufferedContract posts one buffered request against the served routes
// and asserts the reply is a 200 carrying every wanted fragment.
func bufferedContract(t *testing.T, path, body string, wants ...string) {
	t.Helper()
	rec := post(t, servedRoutes(t), path, "", body)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	assertContains(t, rec.Body.String(), wants...)
}

func TestResponsesRouteNonStream(t *testing.T) {
	t.Parallel()
	bufferedContract(t, "/v1/responses", `{"model":"m1","input":"hello"}`,
		`"object":"response"`, `"text":"hi"`)
}

// assertContains fails the test once for every wanted substring the
// body lacks.
func assertContains(t *testing.T, body string, wants ...string) {
	t.Helper()
	for _, want := range wants {
		if !strings.Contains(body, want) {
			t.Errorf("stream missing %q\ngot:\n%s", want, body)
		}
	}
}

// streamContract posts one streaming request against the served routes
// and asserts the reply carries every event of the format's stream
// contract.
func streamContract(t *testing.T, path, body string, wants ...string) {
	t.Helper()
	rec := post(t, servedRoutes(t), path, "", body)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	assertContains(t, rec.Body.String(), wants...)
}

func TestResponsesRouteStream(t *testing.T) {
	t.Parallel()
	streamContract(t, "/v1/responses", `{"model":"m1","stream":true,"input":"hello"}`,
		"event: response.created",
		"event: response.output_text.delta",
		`"delta":"hi"`,
		"event: response.completed",
		`"output_tokens":1`,
	)
}

func TestMessagesRouteNonStream(t *testing.T) {
	t.Parallel()
	bufferedContract(t, "/v1/messages",
		`{"model":"m1","max_tokens":64,"messages":[{"role":"user","content":"hello"}]}`,
		`"type":"message"`, `"stop_reason":"end_turn"`)
}

func TestMessagesRouteStream(t *testing.T) {
	t.Parallel()
	streamContract(t, "/v1/messages",
		`{"model":"m1","stream":true,"max_tokens":64,"messages":[{"role":"user","content":"hello"}]}`,
		"event: message_start",
		"event: content_block_delta",
		`"text":"hi"`,
		"event: message_stop",
	)
}

func TestMessagesRouteHonorsXAPIKey(t *testing.T) {
	t.Parallel()
	backend := testUpstreamBackend(t)
	defer backend.Close()

	identity := staticIdentity(t)
	adapter, err := upstream.NewOpenAI(upstream.OpenAIConfig{ID: "test", BaseURL: backend.URL})
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	priority, err := router.NewPriority([]router.Binding{{Models: []string{"*"}, Upstream: adapter}})
	if err != nil {
		t.Fatalf("router: %v", err)
	}
	relayer := relay.New(retry.Policy{MaxAttempts: 1}, retry.NewBudget(8))
	handler := pipeline.Chain(
		pipeline.CarrierStage(),
		pipeline.FormatStage(protocol.FormatAnthropicMessages),
		pipeline.AuthStage(identity),
	)(NewInference(protocol.FormatAnthropicMessages, priority, relayer))

	req := httptest.NewRequest(http.MethodPost, "/v1/messages",
		strings.NewReader(`{"model":"m1","max_tokens":64,"messages":[{"role":"user","content":"hello"}]}`))
	req.Header.Set("x-api-key", keyT2)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("x-api-key auth status = %d body = %s", rec.Code, rec.Body.String())
	}

	// A missing key gets the Messages error envelope, not the OpenAI one.
	req = httptest.NewRequest(http.MethodPost, "/v1/messages",
		strings.NewReader(`{"model":"m1","max_tokens":64,"messages":[{"role":"user","content":"hello"}]}`))
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized || !strings.Contains(rec.Body.String(), `"type":"error"`) {
		t.Fatalf("unauthorized = %d %s", rec.Code, rec.Body.String())
	}
}

// TestMessagesRouteStreamAbortTerminatesHonestly is the termination
// evidence in the Messages format: delivered deltas stay, one error
// event closes.
func TestMessagesRouteStreamAbortTerminatesHonestly(t *testing.T) {
	t.Parallel()
	backend := abortingUpstreamBackend(t)
	defer backend.Close()
	handler := buildAllRoutes(t, backend.URL)

	rec := post(t, handler, "/v1/messages", "",
		`{"model":"m1","stream":true,"max_tokens":64,"messages":[{"role":"user","content":"hello"}]}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"text":"hi"`) {
		t.Fatalf("delivered delta lost: %s", body)
	}
	if !strings.Contains(body, "event: error") || !strings.Contains(body, `"type":"api_error"`) {
		t.Fatalf("honest termination missing: %s", body)
	}
	if strings.Contains(body, "event: message_stop") {
		t.Fatalf("an aborted stream must not end with message_stop: %s", body)
	}
}

func TestMessagesRouteRejectsMissingMaxTokens(t *testing.T) {
	t.Parallel()
	rec := post(t, servedRoutes(t), "/v1/messages", "", `{"model":"m1","messages":[{"role":"user","content":"hello"}]}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "max_tokens") {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

// staticIdentity builds the auth store used by the governance tests.
func staticIdentity(t *testing.T) auth.Store {
	t.Helper()
	identity, err := auth.NewStatic(auth.StaticConfig{
		Tiers:   []auth.StaticTier{{ID: "free", RPM: 100, TPM: 1_000_000, MaxTokens: 50, MonthlyQuota: 100_000, AllowedModels: []string{"*"}}},
		Tenants: []auth.StaticTenant{{ID: "t2", Name: "T2", Tier: "free", Keys: []string{keyT2}}},
	})
	if err != nil {
		t.Fatalf("identity: %v", err)
	}
	return identity
}

// abortingUpstreamBackend streams one chunk then cuts the connection.
func abortingUpstreamBackend(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher := w.(http.Flusher)
		_, _ = w.Write([]byte(`data: {"choices":[{"delta":{"content":"hi"}}]}` + "\n\n"))
		flusher.Flush()
		// Truncated stream: no [DONE], connection dies.
		panic(http.ErrAbortHandler)
	}))
}
