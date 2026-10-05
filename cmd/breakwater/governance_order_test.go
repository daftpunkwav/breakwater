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
	"net/http"
	"strings"
	"testing"
)

func TestCacheReplayCannotBypassTierModelAuthorization(t *testing.T) {
	backend, hits := newCompletionBackend(t)

	identity := `{"tiers":[` +
		`{"id":"wide","rpm":100,"tpm":100000,"max_tokens":64,"monthly_quota":1000000,"allowed_models":["m1","m2"]},` +
		`{"id":"narrow","rpm":100,"tpm":100000,"max_tokens":64,"monthly_quota":1000000,"allowed_models":["m1"]}],` +
		`"tenants":[` +
		`{"id":"tw","name":"Wide","tier":"wide","keys":["kw"]},` +
		`{"id":"tn","name":"Narrow","tier":"narrow","keys":["kn"]}]}`
	setEnv(t, append(baseEnv("127.0.0.1:0"),
		upstreamEnv("mock", backend.URL, `"*"`),
		"BREAKWATER_IDENTITY="+identity,
	))
	addr, stop := runGateway(t)

	// Cache-eligible body: explicitly deterministic parameters.
	const body = `{"model":"m2","temperature":0,"messages":[{"role":"user","content":"hello"}]}`

	// The wide tenant may call m2; the exchange populates the cache.
	if resp := postCompletion(t, addr, "kw", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("wide tenant status = %d, want 200", resp.StatusCode)
	}
	if *hits != 1 {
		t.Fatalf("upstream hits = %d, want 1 after the wide tenant's fetch", *hits)
	}

	// The narrow tenant sends the byte-identical body: a cache hit by
	// key, but its tier denies m2. Authorization must refuse before the
	// cache stage is ever consulted — a replay may not launder the
	// model past the tier's allow list.
	resp := postCompletion(t, addr, "kn", body)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("narrow tenant status = %d, want 403", resp.StatusCode)
	}
	buf := make([]byte, 512)
	n, _ := resp.Body.Read(buf)
	if !strings.Contains(string(buf[:n]), "model_not_allowed") {
		t.Fatalf("body = %s, want the model_not_allowed envelope", string(buf[:n]))
	}
	if *hits != 1 {
		t.Fatalf("upstream hits = %d, want 1 (the denied request touched nothing)", *hits)
	}

	stop()
}
