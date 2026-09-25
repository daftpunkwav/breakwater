/**
 * @file static_identity_test
 * @description The static identity store: assembly-time rejection of
 * broken identity sets, key resolution, the tenant listing and the
 * per-id lookup, plus parsing the raw identity JSON.
 */
package auth

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

// validStaticConfig is a healthy two-tier identity set with a
// multi-key tenant.
func validStaticConfig() StaticConfig {
	return StaticConfig{
		Tiers: []StaticTier{
			{ID: "free", RPM: 60, TPM: 90_000, MaxTokens: 4096, MonthlyQuota: 1_000_000, AllowedModels: []string{"m1"}},
			{ID: "pro", RPM: 600, TPM: 900_000, AllowedModels: []string{"*"}},
		},
		Tenants: []StaticTenant{
			{ID: "tenant-1", Name: "Acme", Tier: "free", Keys: []string{"sk-1", "sk-1b"}},
			{ID: "tenant-2", Name: "Beta", Tier: "pro", Keys: []string{"sk-2"}},
		},
	}
}

// TestNewStaticRejectsBrokenIdentitySets: every broken identity shape
// fails at assembly, before a request can ride through.
func TestNewStaticRejectsBrokenIdentitySets(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*StaticConfig)
		wantMsg string
	}{
		{"tier without id", func(c *StaticConfig) { c.Tiers[0].ID = "" }, "tier without id"},
		{"tenant without id", func(c *StaticConfig) { c.Tenants[0].ID = "" }, "tenant without id"},
		{"unknown tier reference", func(c *StaticConfig) { c.Tenants[0].Tier = "missing" }, "unknown tier"},
		{"unknown role", func(c *StaticConfig) { c.Tenants[0].Role = "owner" }, "unknown role"},
		{"empty api key", func(c *StaticConfig) { c.Tenants[0].Keys[0] = "" }, "empty api key"},
		{"duplicate api key", func(c *StaticConfig) { c.Tenants[1].Keys[0] = "sk-1" }, "duplicate api key"},
		{"duplicate api key within a tenant", func(c *StaticConfig) { c.Tenants[0].Keys[1] = "sk-1" }, "duplicate api key"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := validStaticConfig()
			tc.mutate(&cfg)

			store, err := NewStatic(cfg)
			if err == nil {
				t.Fatalf("NewStatic accepted a broken set, store %+v", store)
			}
			if !strings.Contains(err.Error(), tc.wantMsg) {
				t.Fatalf("err = %q, want it to mention %q", err, tc.wantMsg)
			}
		})
	}
}

// TestStaticResolve: a configured key resolves to its tenant snapshot
// with the tier attached; any other key is a definitive refusal.
func TestStaticResolve(t *testing.T) {
	t.Parallel()

	store, err := NewStatic(validStaticConfig())
	if err != nil {
		t.Fatalf("NewStatic: %v", err)
	}

	// Both keys of a multi-key tenant resolve to the same identity.
	for _, key := range []string{"sk-1", "sk-1b"} {
		tenant, err := store.Resolve(context.Background(), key)
		if err != nil {
			t.Fatalf("Resolve(%q): %v", key, err)
		}
		if tenant.ID != "tenant-1" || tenant.Name != "Acme" {
			t.Fatalf("Resolve(%q) = %+v, want tenant-1/Acme", key, tenant)
		}
		if tenant.Tier.ID != "free" || tenant.Tier.RPM != 60 {
			t.Fatalf("tier snapshot = %+v, want the free tier", tenant.Tier)
		}
	}

	if _, err := store.Resolve(context.Background(), "sk-unknown"); err == nil {
		t.Fatal("Resolve accepted an unconfigured key")
	} else if err != ErrUnauthorized {
		t.Fatalf("err = %v, want exactly ErrUnauthorized", err)
	}
}

// TestStaticTenantsSortedAndDeduped: the listing carries every tenant
// id exactly once, in stable sorted order.
func TestStaticTenantsSortedAndDeduped(t *testing.T) {
	t.Parallel()

	store, err := NewStatic(validStaticConfig())
	if err != nil {
		t.Fatalf("NewStatic: %v", err)
	}

	got := store.Tenants()
	want := []string{"tenant-1", "tenant-2"}
	if len(got) != len(want) {
		t.Fatalf("Tenants() = %v, want %v (deduped)", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("Tenants() = %v, want %v", got, want)
		}
	}
}

// TestStaticTenantByID: the id lookup finds a configured tenant and
// reports a miss honestly.
func TestStaticTenantByID(t *testing.T) {
	t.Parallel()

	store, err := NewStatic(validStaticConfig())
	if err != nil {
		t.Fatalf("NewStatic: %v", err)
	}

	tenant, ok := store.TenantByID("tenant-2")
	if !ok || tenant.ID != "tenant-2" || tenant.Tier.ID != "pro" {
		t.Fatalf("TenantByID = (%+v, %v), want tenant-2 on the pro tier", tenant, ok)
	}

	if tenant, ok := store.TenantByID("nobody"); ok || !reflect.DeepEqual(tenant, Tenant{}) {
		t.Fatalf("TenantByID(nobody) = (%+v, %v), want a miss", tenant, ok)
	}
}

// TestParseStaticConfigRejectsGarbage: malformed identity JSON fails
// with the parse wrap, not silently.
func TestParseStaticConfigRejectsGarbage(t *testing.T) {
	t.Parallel()

	if _, err := ParseStaticConfig([]byte("{not json")); err == nil {
		t.Fatal("ParseStaticConfig accepted garbage")
	} else if !strings.Contains(err.Error(), "parse identity") {
		t.Fatalf("err = %q, want the parse-identity wrap", err)
	}
}

// TestParseStaticConfigDecodesSchema: the decoded config keeps the
// documented JSON field names.
func TestParseStaticConfigDecodesSchema(t *testing.T) {
	t.Parallel()

	raw := `{
		"tiers": [{"id":"free","rpm":60,"tpm":90000,"max_tokens":4096,"monthly_quota":1000000,"allowed_models":["m1"]}],
		"tenants": [{"id":"t1","name":"Acme","tier":"free","keys":["sk-1"]}]
	}`
	cfg, err := ParseStaticConfig([]byte(raw))
	if err != nil {
		t.Fatalf("ParseStaticConfig: %v", err)
	}
	if len(cfg.Tiers) != 1 || cfg.Tiers[0].ID != "free" || cfg.Tiers[0].RPM != 60 {
		t.Fatalf("tiers = %+v", cfg.Tiers)
	}
	if len(cfg.Tenants) != 1 || cfg.Tenants[0].ID != "t1" || len(cfg.Tenants[0].Keys) != 1 {
		t.Fatalf("tenants = %+v", cfg.Tenants)
	}
}
