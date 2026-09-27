/**
 * @file routing_config_test
 * @description The assembly-time validation of the model-level routing
 * configuration: fallback chains and context ceilings must name
 * client-facing models the configuration actually serves — resolved
 * with the router's own semantics, so a wildcard binding serves any
 * concrete name.
 */
package main

import (
	"strings"
	"testing"

	"github.com/daftpunkwav/breakwater/internal/config"
)

func TestValidateModelNames(t *testing.T) {
	t.Parallel()
	upstreams := []config.Upstream{{ID: "a", BaseURL: "http://a", Models: []string{"m1", "m2"}}}

	if err := validateModelNames(upstreams, map[string][]string{"m1": {"m2"}}, map[string]int64{"m2": 100}); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	if err := validateModelNames(upstreams, nil, nil); err != nil {
		t.Fatalf("empty config rejected: %v", err)
	}

	if err := validateModelNames(upstreams, map[string][]string{"typo": {"m2"}}, nil); err == nil ||
		!strings.Contains(err.Error(), "fallback key") {
		t.Fatalf("err = %v, want a fallback-key rejection", err)
	}
	if err := validateModelNames(upstreams, map[string][]string{"m1": {"ghost"}}, nil); err == nil ||
		!strings.Contains(err.Error(), "fallback target") {
		t.Fatalf("err = %v, want a fallback-target rejection", err)
	}
	if err := validateModelNames(upstreams, nil, map[string]int64{"ghost": 10}); err == nil ||
		!strings.Contains(err.Error(), "context limit") {
		t.Fatalf("err = %v, want a context-limit rejection", err)
	}
}

// TestValidateModelNamesWildcard: a wildcard binding serves every
// concrete name — fallback chains and ceilings stay bootable under the
// catch-all configuration, and a wildcard entry itself is never a
// requestable model.
func TestValidateModelNamesWildcard(t *testing.T) {
	t.Parallel()
	wildcards := []config.Upstream{{ID: "a", BaseURL: "http://a", Models: []string{"*"}}}

	if err := validateModelNames(wildcards, map[string][]string{"gpt-4o": {"gpt-4o-mini"}}, map[string]int64{"gpt-4o": 1000}); err != nil {
		t.Fatalf("wildcard config rejected: %v", err)
	}
	if servesModel(wildcards, "*") {
		t.Fatal("the wildcard itself must not read as a requestable model")
	}
}

// TestAdminAuthStateNamesThePosture: the startup log must be able to
// tell an open admin surface from a token-guarded one at a glance.
func TestAdminAuthStateNamesThePosture(t *testing.T) {
	t.Parallel()
	if got := adminAuthState(""); !strings.Contains(got, "open") {
		t.Fatalf("empty token = %q, want the open-surface admission", got)
	}
	if got := adminAuthState("tok"); got != "bearer token" {
		t.Fatalf("tokened = %q, want the bearer-token posture", got)
	}
}

// TestWildcardServedDetectsTheCatchAll: any binding carrying the "*"
// wildcard arms wildcard mode; plain lists do not.
func TestWildcardServedDetectsTheCatchAll(t *testing.T) {
	t.Parallel()
	wildcards := []config.Upstream{{ID: "a", BaseURL: "http://a", Models: []string{"gpt-4o", "*"}}}
	if !wildcardServed(wildcards) {
		t.Fatal("a wildcard binding must arm wildcard mode")
	}
	named := []config.Upstream{{ID: "a", BaseURL: "http://a", Models: []string{"gpt-4o"}}}
	if wildcardServed(named) {
		t.Fatal("a plain list must not arm wildcard mode")
	}
}
