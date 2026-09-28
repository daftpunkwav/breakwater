/**
 * @file governance_order_test
 * @description The assembled governance stage order, locked at the
 * composition root through a real serve(): the cache key is the request
 * body alone, so the model-authorization stage must sit before the
 * cache stage — a replayed entry may never serve a model the
 * requesting tenant's tier forbids. A reorder of the stage list in
 * serve.go fails here.
 */
package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/daftpunkwav/breakwater/internal/config"
)

func TestCacheReplayCannotBypassTierModelAuthorization(t *testing.T) {
	hits := 0
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"hi"}}],` +
			`"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`))
	}))
	defer backend.Close()

	entries := append(baseEnv("127.0.0.1:0"),
		`BREAKWATER_UPSTREAMS=[{"id":"mock","base_url":"`+backend.URL+`","models":["*"]}]`,
		`BREAKWATER_IDENTITY={"tiers":[`+
			`{"id":"wide","rpm":100,"tpm":100000,"max_tokens":64,"monthly_quota":1000000,"allowed_models":["m1","m2"]},`+
			`{"id":"narrow","rpm":100,"tpm":100000,"max_tokens":64,"monthly_quota":1000000,"allowed_models":["m1"]}],`+
			`"tenants":[`+
			`{"id":"tw","name":"Wide","tier":"wide","keys":["kw"]},`+
			`{"id":"tn","name":"Narrow","tier":"narrow","keys":["kn"]}]}`,
	)
	setEnv(t, entries)
	cfg, err := config.Load()
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("bind listener: %v", err)
	}
	defer func() { _ = listener.Close() }()
	addr := listener.Addr().String()
	cfg.Server.Addr = addr // ignored: the listener is served instead

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- serve(ctx, cfg, slog.New(slog.DiscardHandler), "test", listener) }()

	// Wait for the gateway to accept traffic.
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := http.Get("http://" + addr + "/healthz")
		if err == nil {
			_ = resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("gateway never came up: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	post := func(key, body string) *http.Response {
		t.Helper()
		req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/v1/chat/completions",
			strings.NewReader(body))
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		req.Header.Set("Authorization", "Bearer "+key)
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("post: %v", err)
		}
		t.Cleanup(func() { _ = resp.Body.Close() })
		return resp
	}

	// Cache-eligible body: explicitly deterministic parameters.
	const body = `{"model":"m2","temperature":0,"messages":[{"role":"user","content":"hello"}]}`

	// The wide tenant may call m2; the exchange populates the cache.
	if resp := post("kw", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("wide tenant status = %d, want 200", resp.StatusCode)
	}
	if hits != 1 {
		t.Fatalf("upstream hits = %d, want 1 after the wide tenant's fetch", hits)
	}

	// The narrow tenant sends the byte-identical body: a cache hit by
	// key, but its tier denies m2. Authorization must refuse before the
	// cache stage is ever consulted — a replay may not launder the
	// model past the tier's allow list.
	resp := post("kn", body)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("narrow tenant status = %d, want 403", resp.StatusCode)
	}
	buf := make([]byte, 512)
	n, _ := resp.Body.Read(buf)
	if !strings.Contains(string(buf[:n]), "model_not_allowed") {
		t.Fatalf("body = %s, want the model_not_allowed envelope", string(buf[:n]))
	}
	if hits != 1 {
		t.Fatalf("upstream hits = %d, want 1 (the denied request touched nothing)", hits)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("serve: %v", err)
	}
}
