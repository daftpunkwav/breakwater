/**
 * @file upstreams_test
 * @description The upstream table's assembly-time guards: duplicated
 * ids and base URLs without an http(s) scheme or host refuse to boot —
 * both would otherwise surface as per-request failures against a
 * "healthy" gateway.
 */
package config

import (
	"strings"
	"testing"
)

func loadWithUpstreams(t *testing.T, raw string) error {
	t.Helper()
	t.Setenv("BREAKWATER_UPSTREAMS", raw)
	_, err := Load()
	return err
}

func TestLoadRejectsDuplicateUpstreamID(t *testing.T) {
	err := loadWithUpstreams(t,
		`[{"id":"a","base_url":"http://a:1","models":["m1"]},{"id":"a","base_url":"http://b:2","models":["m2"]}]`)
	if err == nil || !strings.Contains(err.Error(), "repeats id") {
		t.Fatalf("err = %v, want a duplicate-id rejection", err)
	}
}

func TestLoadRejectsBaseURLWithoutScheme(t *testing.T) {
	for name, raw := range map[string]string{
		"missing scheme": `[{"id":"a","base_url":"mockllm:8090","models":["m1"]}]`,
		"missing host":   `[{"id":"a","base_url":"http://","models":["m1"]}]`,
	} {
		if err := loadWithUpstreams(t, raw); err == nil || !strings.Contains(err.Error(), "base_url") {
			t.Errorf("%s: err = %v, want a base_url rejection", name, err)
		}
	}
}

func TestLoadAcceptsValidUpstreams(t *testing.T) {
	// A valid upstream table needs an identity source alongside it (the
	// unauthenticated-deployment guard); this test is about the table
	// itself, so it arms the static set minimally.
	t.Setenv("BREAKWATER_IDENTITY", `{"tiers":[{"id":"free","allowed_models":["*"]}],"tenants":[{"id":"t","tier":"free","keys":["k"]}]}`)
	if err := loadWithUpstreams(t,
		`[{"id":"a","base_url":"http://127.0.0.1:8090","models":["*"]}]`); err != nil {
		t.Fatalf("valid upstreams rejected: %v", err)
	}
}

// TestLoadRejectsUnknownUpstreamMember pins the strict-decode contract:
// a typo'd member (say "api_keey") must fail the load instead of
// silently yielding an upstream without credentials.
func TestLoadRejectsUnknownUpstreamMember(t *testing.T) {
	t.Setenv("BREAKWATER_IDENTITY", `{"tiers":[{"id":"free","allowed_models":["*"]}],"tenants":[{"id":"t","tier":"free","keys":["k"]}]}`)
	err := loadWithUpstreams(t,
		`[{"id":"a","base_url":"http://127.0.0.1:8090","models":["*"],"api_keey":"k"}]`)
	if err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("err = %v, want an unknown-member rejection", err)
	}
}

// TestLoadRejectsInvalidUpstreamID pins the identifier charset: an
// upstream id is a Redis key segment, a metrics label and an admin URL
// segment, so a "/" or a space must refuse to boot.
func TestLoadRejectsInvalidUpstreamID(t *testing.T) {
	for name, raw := range map[string]string{
		"slash":     `[{"id":"a/b","base_url":"http://x","models":["*"]}]`,
		"space":     `[{"id":"a b","base_url":"http://x","models":["*"]}]`,
		"too long":  `[{"id":"` + strings.Repeat("a", 129) + `","base_url":"http://x","models":["*"]}]`,
		"other set": `[{"id":"a:b","base_url":"http://x","models":["*"]}]`,
	} {
		if err := loadWithUpstreams(t, raw); err == nil || !strings.Contains(err.Error(), "invalid id") {
			t.Errorf("%s: err = %v, want an invalid-id rejection", name, err)
		}
	}
}

func TestLoadRejectsBrokenCredentialRings(t *testing.T) {
	for name, raw := range map[string]string{
		"empty api_keys entry":   `[{"id":"a","base_url":"http://x","api_keys":["k1",""],"models":["*"]}]`,
		"duplicate in api_keys":  `[{"id":"a","base_url":"http://x","api_keys":["k1","k1"],"models":["*"]}]`,
		"api_key repeated later": `[{"id":"a","base_url":"http://x","api_key":"k1","api_keys":["k2","k1"],"models":["*"]}]`,
	} {
		if err := loadWithUpstreams(t, raw); err == nil {
			t.Errorf("%s: accepted a broken credential ring", name)
		}
	}
}

// TestCredentialsMergeOrder pins the merged ring order: api_key leads,
// api_keys follow, an empty api_key leaves the ring to api_keys alone.
func TestCredentialsMergeOrder(t *testing.T) {
	merged := Upstream{APIKey: "k1", APIKeys: []string{"k2", "k3"}}.Credentials()
	if len(merged) != 3 || merged[0] != "k1" || merged[1] != "k2" || merged[2] != "k3" {
		t.Fatalf("merged = %v, want k1,k2,k3", merged)
	}
	only := Upstream{APIKeys: []string{"k2"}}.Credentials()
	if len(only) != 1 || only[0] != "k2" {
		t.Fatalf("merged = %v, want k2 alone", only)
	}
	if got := (Upstream{}).Credentials(); len(got) != 0 {
		t.Fatalf("merged = %v, want empty for a keyless upstream", got)
	}
}
