/**
 * @file modelmap_test
 * @description Client-to-provider model rewrites: parsing the
 * "client=real" bindings, the body rewrite on mapped requests and the
 * verbatim passthrough on everything else.
 */
package upstream

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
)

// TestParseModelMap: plain entries stay out of the map, "client=real"
// entries map, malformed entries are ignored (config rejects them
// first; this is defense in depth).
func TestParseModelMap(t *testing.T) {
	t.Parallel()

	got := ParseModelMap([]string{"gpt-4o=deepseek-chat", "deepseek-reasoner", "=broken", "broken=", "*=wild"})
	want := map[string]string{"gpt-4o": "deepseek-chat"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ParseModelMap = %v, want %v", got, want)
	}
}

// rewriteStub is the provider side of the model-map rewrite tests: a
// server answering `{}` that records every request body, and an
// adapter bound to the gpt-4o=deepseek-chat mapping.
type rewriteStub struct {
	server  *httptest.Server
	adapter *OpenAI
	// lastBody yields the most recent request body the stub received.
	lastBody func() string
	// count yields the number of exchanges the stub served.
	count func() int
}

// newRewriteStub wires the stub; the server closes when the test ends.
func newRewriteStub(t *testing.T) *rewriteStub {
	t.Helper()
	var bodies []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(raw))
		_, _ = w.Write([]byte(`{}`))
	}))
	t.Cleanup(server.Close)

	adapter := mustOpenAI(t, OpenAIConfig{
		ID:       "up-1",
		BaseURL:  server.URL,
		ModelMap: ParseModelMap([]string{"gpt-4o=deepseek-chat"}),
	})
	return &rewriteStub{
		server:  server,
		adapter: adapter,
		lastBody: func() string {
			if len(bodies) == 0 {
				return ""
			}
			return bodies[len(bodies)-1]
		},
		count: func() int { return len(bodies) },
	}
}

// TestForwardRewritesMappedModel: a mapped client model reaches the
// provider under its real name, with every other body member kept.
func TestForwardRewritesMappedModel(t *testing.T) {
	t.Parallel()

	stub := newRewriteStub(t)
	body := `{"model":"gpt-4o","messages":[{"role":"user","content":"a < b & c"}],"temperature":0.5}`
	if _, err := stub.adapter.Forward(context.Background(), Request{Model: "gpt-4o", Body: []byte(body)}); err != nil {
		t.Fatalf("Forward: %v", err)
	}
	got := stub.lastBody()

	var decoded map[string]any
	if err := json.Unmarshal([]byte(got), &decoded); err != nil {
		t.Fatalf("rewritten body is not JSON: %v\n%s", err, got)
	}
	if decoded["model"] != "deepseek-chat" {
		t.Fatalf("model = %v, want deepseek-chat", decoded["model"])
	}
	if decoded["temperature"] != 0.5 {
		t.Fatalf("temperature = %v, want 0.5 (other members must survive)", decoded["temperature"])
	}
	// The rewrite must not HTML-escape message text.
	if !strings.Contains(got, `a < b & c`) {
		t.Fatalf("message text was escaped: %s", got)
	}
}

// TestForwardKeepsUnmappedModelVerbatim: without a mapping the body is
// forwarded byte-for-byte — the canonical passthrough contract.
func TestForwardKeepsUnmappedModelVerbatim(t *testing.T) {
	t.Parallel()

	stub := newRewriteStub(t)
	body := `{"model":"other","messages":[],"nested":{"model":"inner"}}`
	if _, err := stub.adapter.Forward(context.Background(), Request{Model: "other", Body: []byte(body)}); err != nil {
		t.Fatalf("Forward: %v", err)
	}
	if got := stub.lastBody(); got != body {
		t.Fatalf("body = %s, want verbatim %s", got, body)
	}

	// A mapping to the same name also skips the rewrite.
	identity := mustOpenAI(t, OpenAIConfig{
		ID:       "up-id",
		BaseURL:  stub.server.URL,
		ModelMap: ParseModelMap([]string{"same=same"}),
	})
	if _, err := identity.Forward(context.Background(), Request{Model: "same", Body: []byte(`{"model":"same"}`)}); err != nil {
		t.Fatalf("Forward: %v", err)
	}
	if got := stub.lastBody(); got != `{"model":"same"}` {
		t.Fatalf("body = %s, want verbatim passthrough for identity mapping", got)
	}
}

// TestForwardRewriteFailsClosed: a mapped model over a non-JSON body
// fails the attempt (the retry loop may fail over) instead of sending
// the unrewritten body.
func TestForwardRewriteFailsClosed(t *testing.T) {
	t.Parallel()

	stub := newRewriteStub(t)
	resp, err := stub.adapter.Forward(context.Background(), Request{Model: "gpt-4o", Body: []byte("not json")})
	if err == nil {
		t.Fatalf("expected a rewrite failure, got response %v", resp)
	}
	if got := stub.count(); got != 0 {
		t.Fatalf("provider saw %d exchanges, want 0", got)
	}
}
