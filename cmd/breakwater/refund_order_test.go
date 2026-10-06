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
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestCacheHitRefundsTheWholeReservation(t *testing.T) {
	backend, hits := newCompletionBackend(t)

	// rpm=2: exactly the two requests below spend the bucket (refunds
	// return TPM tokens only), so the third request is refused by the
	// limiter before any downstream stage runs.
	identity := `{"tiers":[` +
		`{"id":"free","rpm":2,"tpm":100000,"max_tokens":64,"monthly_quota":1000000,"allowed_models":["m1"]}],` +
		`"tenants":[{"id":"t1","name":"T1","tier":"free","keys":["k1"]}]}`
	setEnv(t, append(baseEnv("127.0.0.1:0"),
		upstreamEnv("mock", backend.URL, `"m1"`),
		"BREAKWATER_IDENTITY="+identity,
	))
	addr, stop := runGateway(t)

	// Cache-eligible body: explicitly deterministic parameters.
	const fill = `{"model":"m1","temperature":0,"messages":[{"role":"user","content":"hello"}]}`

	reservedAfterFill := fillCache(t, addr, "k1", fill)
	reserved, refunded := replayFromCache(t, addr, "k1", fill, hits, reservedAfterFill)

	// The rpm bucket is spent: the next request is refused by the
	// limiter stage, and must not take a quota reservation on its way
	// out — the limiter sits before the quota stage.
	other := `{"model":"m1","temperature":0,"messages":[{"role":"user","content":"different"}]}`
	assertRefusalLeavesLedgerUntouched(t, addr, "k1", other, reserved, refunded)

	stop()
}

// fillCache performs the one real upstream fetch that populates the
// cache and returns the reservation total it left behind.
func fillCache(t *testing.T, addr, key, body string) float64 {
	t.Helper()
	if resp := postCompletion(t, addr, key, body); resp.StatusCode != http.StatusOK {
		t.Fatalf("fill status = %d, want 200", resp.StatusCode)
	}
	reserved, _ := scrapeQuotaCounters(t, addr, "t1")
	return reserved
}

// replayFromCache sends the byte-identical body again and pins the
// cache-hit contract: the hit takes a quota lease of its own and
// cancels it in full, so the ledger converges to the two tokens the
// one real fetch consumed. Returns the converged counters.
func replayFromCache(t *testing.T, addr, key, body string, hits *int, reservedAfterFill float64) (reserved, refunded float64) {
	t.Helper()
	if resp := postCompletion(t, addr, key, body); resp.StatusCode != http.StatusOK {
		t.Fatalf("hit status = %d, want 200", resp.StatusCode)
	}
	if *hits != 1 {
		t.Fatalf("upstream hits = %d, want 1", *hits)
	}

	// The hit took a lease of its own: cache sits inside the quota
	// stage, so a replay is reserved too, never free. The reservation
	// is taken before the response is written, so it is already visible.
	reserved, _ = scrapeQuotaCounters(t, addr, "t1")
	if reserved <= reservedAfterFill {
		t.Fatalf("reserved = %v after the cache hit, want more than the fill's %v — the hit escaped the ledger",
			reserved, reservedAfterFill)
	}

	// Any drift means the hit settled instead of cancelling — the LIFO
	// refund is broken. The correction runs after next() returns, and a
	// replayed hit flushes its reply before that, so the refund becomes
	// visible a moment after the client sees the response: poll for the
	// ledger to converge instead of racing it.
	deadline := time.Now().Add(5 * time.Second)
	for {
		reserved, refunded = scrapeQuotaCounters(t, addr, "t1")
		if reserved-refunded == 2 {
			return reserved, refunded
		}
		if time.Now().After(deadline) {
			t.Fatalf("reserved-refunded = %v after the cache hit, want 2 (the real fetch's usage): reserved=%v refunded=%v",
				reserved-refunded, reserved, refunded)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// assertRefusalLeavesLedgerUntouched sends a body the spent rpm bucket
// must refuse and pins that the rejection moved no counter.
func assertRefusalLeavesLedgerUntouched(t *testing.T, addr, key, body string, reserved, refunded float64) {
	t.Helper()
	if resp := postCompletion(t, addr, key, body); resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("over-budget status = %d, want 429", resp.StatusCode)
	}
	reserved2, refunded2 := scrapeQuotaCounters(t, addr, "t1")
	if reserved2 != reserved || refunded2 != refunded {
		t.Fatalf("rejected request moved the ledger: reserved %v->%v refunded %v->%v, want no movement",
			reserved, reserved2, refunded, refunded2)
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
