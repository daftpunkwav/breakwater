/**
 * @file charset_lockstep_test
 * @description The one identifier character set three packages enforce
 * independently: a tenant id is a Redis key segment, a ledger identity,
 * an access-log field and an admin URL segment at once, so the auth,
 * config and server regexes must accept exactly the same strings. Each
 * package keeps its own copy on purpose (no imports between them),
 * which leaves only comments to hold them in lockstep — this test is
 * that comment in executable form.
 */
package server

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/daftpunkwav/breakwater/internal/auth"
	"github.com/daftpunkwav/breakwater/internal/config"
)

// charsetSamples is the shared sample set every surface is judged
// against: the documented set's corners and the characters just outside
// it that would break a key segment, a label or a URL route.
var charsetSamples = []struct {
	name  string
	id    string
	valid bool
}{
	{"single character", "a", true},
	{"every documented character", "A9._-z", true},
	{"exactly 128 characters", strings.Repeat("a", 128), true},
	{"empty", "", false},
	{"space", "a b", false},
	{"slash", "a/b", false},
	{"tilde", "a~b", false},
	{"colon", "a:b", false},
	{"non-ascii", "ténant", false},
	{"129 characters", strings.Repeat("a", 129), false},
}

// TestIdentifierCharsetsStayInLockstep drives one sample set through
// all three enforcement surfaces: the admin path-segment rule, the
// static identity store's tenant ids, and the config loader's upstream
// ids. A drift between them would admit an id one surface can construct
// and another cannot route or store.
func TestIdentifierCharsetsStayInLockstep(t *testing.T) {
	for _, tc := range charsetSamples {
		t.Run(tc.name, func(t *testing.T) {
			// Not parallel: the config surface runs a full Load, and
			// t.Setenv forbids parallel tests.
			// The admin surface's path-segment rule.
			if got := validTenantID(tc.id); got != tc.valid {
				t.Fatalf("server: validTenantID(%q) = %v, want %v", tc.id, got, tc.valid)
			}

			// The static identity store's tenant ids. An empty id is
			// refused by its own assembly guard (the same lower bound
			// the character set encodes), so either rejection message
			// speaks for the rule.
			identity, err := json.Marshal(auth.StaticConfig{
				Tiers:   []auth.StaticTier{{ID: "free"}},
				Tenants: []auth.StaticTenant{{ID: tc.id, Tier: "free", Keys: []string{"k"}}},
			})
			if err != nil {
				t.Fatalf("identity encode: %v", err)
			}
			_, authErr := auth.NewStatic(mustParseIdentity(t, identity), "pepper")
			if (authErr != nil) == tc.valid {
				t.Fatalf("auth: static tenant id %q err = %v, want rejection = %v", tc.id, authErr, !tc.valid)
			}
			if !tc.valid && !strings.Contains(authErr.Error(), "invalid id") && !strings.Contains(authErr.Error(), "without id") {
				t.Fatalf("auth: id %q rejected by a different rule than the character set: %v", tc.id, authErr)
			}

			// The config loader's upstream ids.
			t.Setenv("BREAKWATER_IDENTITY", `{"tiers":[{"id":"free","allowed_models":["*"]}],"tenants":[{"id":"t","tier":"free","keys":["k"]}]}`)
			t.Setenv("BREAKWATER_ADMIN_TOKEN", "dev-admin")
			t.Setenv("BREAKWATER_UPSTREAMS",
				`[{"id":`+mustJSON(t, tc.id)+`,"base_url":"http://127.0.0.1:8090","models":["*"]}]`)
			_, loadErr := config.Load()
			if (loadErr != nil) == tc.valid {
				t.Fatalf("config: upstream id %q err = %v, want rejection = %v", tc.id, loadErr, !tc.valid)
			}
			// An empty id is refused by the required-field guard just
			// before the character set — the same lower bound, a
			// different message.
			if !tc.valid && !strings.Contains(loadErr.Error(), "invalid id") && !strings.Contains(loadErr.Error(), "needs id") {
				t.Fatalf("config: id %q rejected by a different rule than the character set: %v", tc.id, loadErr)
			}
		})
	}
}

// mustParseIdentity decodes a raw identity document or fails the test.
func mustParseIdentity(t *testing.T, raw []byte) auth.StaticConfig {
	t.Helper()
	cfg, err := auth.ParseStaticConfig(raw)
	if err != nil {
		t.Fatalf("identity parse: %v", err)
	}
	return cfg
}

// mustJSON encodes v as a JSON literal for embedding in a raw config
// document.
func mustJSON(t *testing.T, v any) string {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("encode %v: %v", v, err)
	}
	return string(raw)
}
