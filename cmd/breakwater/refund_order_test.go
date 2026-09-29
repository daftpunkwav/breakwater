/**
 * @file refund_order_test
 * @description The composition-root pin of the refund LIFO: the cache
 * stage sits inside the rate-limit and quota stages, and a cache hit
 * zeroes carrier.Consumed before the outer stages' deferred corrections
 * read it. Nothing in the type system holds this together — the stage
 * list is a literal slice in serve.go and the hand-off rides the
 * carrier — so this test walks a real serve() and fails on the two
 * reorderings that would silently break metering:
 *
 *   - cache moved outside quota (or quota inside cache): a hit stops
 *     reserving and cancelling a lease, so cached traffic escapes the
 *     ledger entirely;
 *   - settlement that no longer reads the carrier after next(): a hit
 *     settles the full estimate instead of cancelling it.
 *
 * Both show up as reserved minus refunded drifting away from the
 * tokens the one real upstream fetch consumed. The rate-limit side is
 * pinned with it: a request the limiter refuses must never reach the
 * quota stage, so a rejection leaves the reservation counter alone.
 */
package main

import (
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/daftpunkwav/breakwater/internal/config"
)

func TestCacheHitRefundsTheWholeReservation(t *testing.T) {
	hits := 0
	backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"choices":[{"message":{"role":"assistant","content":"hi"}}],` +
			`"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}`))
	}))
	defer backend.Close()

	// rpm=2: exactly the two requests below spend the bucket (refunds
	// return TPM tokens only), so the third request is refused by the
	// limiter before any downstream stage runs.
	entries := append(baseEnv("127.0.0.1:0"),
		`BREAKWATER_UPSTREAMS=[{"id":"mock","base_url":"`+backend.URL+`","models":["m1"]}]`,
		`BREAKWATER_IDENTITY={"tiers":[`+
			`{"id":"free","rpm":2,"tpm":100000,"max_tokens":64,"monthly_quota":1000000,"allowed_models":["m1"]}],`+
			`"tenants":[{"id":"t1","name":"T1","tier":"free","keys":["k1"]}]}`,
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

	post := func(body string) *http.Response {
		t.Helper()
		req, err := http.NewRequest(http.MethodPost, "http://"+addr+"/v1/chat/completions",
			strings.NewReader(body))
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		req.Header.Set("Authorization", "Bearer k1")
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("post: %v", err)
		}
		t.Cleanup(func() { _ = resp.Body.Close() })
		return resp
	}

	// Cache-eligible body: explicitly deterministic parameters.
	const fill = `{"model":"m1","temperature":0,"messages":[{"role":"user","content":"hello"}]}`

	// The fetch fills the cache.
	if resp := post(fill); resp.StatusCode != http.StatusOK {
		t.Fatalf("fill status = %d, want 200", resp.StatusCode)
	}
	reserved, refunded := scrapeQuotaCounters(t, addr, "t1")
	reservedAfterFill := reserved

	// The byte-identical body replays from the cache: the hit must
	// reserve a lease (cached traffic is metered) AND cancel it in
	// full — net spend stays at the one real fetch's consumed tokens.
	if resp := post(fill); resp.StatusCode != http.StatusOK {
		t.Fatalf("hit status = %d, want 200", resp.StatusCode)
	}
	if hits != 1 {
		t.Fatalf("upstream hits = %d, want 1", hits)
	}

	reserved, refunded = scrapeQuotaCounters(t, addr, "t1")
	// The hit took a lease of its own: cache sits inside the quota
	// stage, so a replay is reserved too, never free.
	if reserved <= reservedAfterFill {
		t.Fatalf("reserved = %v after the cache hit, want more than the fill's %v — the hit escaped the ledger",
			reserved, reservedAfterFill)
	}
	// The one real fetch consumed 2 tokens (its usage block); the hit
	// consumed nothing. Any drift means the hit settled instead of
	// cancelling — the LIFO refund is broken.
	if got := reserved - refunded; got != 2 {
		t.Fatalf("reserved-refunded = %v after the cache hit, want 2 (the real fetch's usage): reserved=%v refunded=%v",
			got, reserved, refunded)
	}

	// The rpm bucket is spent: the next request is refused by the
	// limiter stage, and must not take a quota reservation on its way
	// out — the limiter sits before the quota stage.
	other := `{"model":"m1","temperature":0,"messages":[{"role":"user","content":"different"}]}`
	if resp := post(other); resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("over-budget status = %d, want 429", resp.StatusCode)
	}
	reserved2, refunded2 := scrapeQuotaCounters(t, addr, "t1")
	if reserved2 != reserved || refunded2 != refunded {
		t.Fatalf("rejected request moved the ledger: reserved %v->%v refunded %v->%v, want no movement",
			reserved, reserved2, refunded, refunded2)
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("serve: %v", err)
	}
}

// scrapeQuotaCounters reads the tenant's reservation and refund
// counters off the /metrics exposition.
func scrapeQuotaCounters(t *testing.T, addr, tenant string) (reserved, refunded float64) {
	t.Helper()
	resp, err := http.Get("http://" + addr + "/metrics")
	if err != nil {
		t.Fatalf("scrape metrics: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	buf := new(strings.Builder)
	_, _ = io.Copy(buf, resp.Body)
	body := buf.String()
	return quotaCounter(t, body, "breakwater_quota_reservation_tokens_total", tenant),
		quotaCounter(t, body, "breakwater_quota_refunded_tokens_total", tenant)
}

// quotaCounter extracts one labeled counter's value from the
// exposition; a missing sample is a failure, not a zero.
func quotaCounter(t *testing.T, body, name, tenant string) float64 {
	t.Helper()
	prefix := name + `{tenant="` + tenant + `"} `
	for _, line := range strings.Split(body, "\n") {
		if !strings.HasPrefix(line, prefix) {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, prefix)), 64)
		if err != nil {
			t.Fatalf("parse %q: %v", line, err)
		}
		return v
	}
	t.Fatalf("metrics exposition missing %s{tenant=%q}:\n%s", name, tenant, body)
	return 0
}
