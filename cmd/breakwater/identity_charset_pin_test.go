/**
 * @file identity_charset_pin_test
 * @description The identifier character-set rule is written three
 * times on purpose: internal/auth guards static tenant construction,
 * internal/config guards upstream ids (server must not import config,
 * and auth's rule guards construction, not the admin surface's input).
 * All three guard one reality — an id is a Redis key segment, a ledger
 * identity, a metrics label, an access-log field and an admin URL
 * segment at once — so the copies must stay in lockstep. The
 * cross-references in the comments cannot enforce that; this pin runs
 * the same corpus through every layer's real entry point and fails
 * when one copy alone is edited.
 */
package main

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"

	"github.com/daftpunkwav/breakwater/internal/auth"
	"github.com/daftpunkwav/breakwater/internal/config"
	"github.com/daftpunkwav/breakwater/internal/obs"
	"github.com/daftpunkwav/breakwater/internal/quota"
)

func TestIdentifierCharsetRuleAgreesAcrossLayers(t *testing.T) {
	valid := []string{"t1", "A.b_c-09", strings.Repeat("a", 128)}
	invalid := []string{
		"",
		strings.Repeat("a", 129),
		"a b",
		"a/b",
		"a~b",
		"a+b",
		"租户",
	}

	// Layer 1: static tenant construction (internal/auth).
	for _, id := range valid {
		if _, err := auth.NewStatic(staticIdentityWithTenant(id)); err != nil {
			t.Errorf("auth.NewStatic(%q) = %v, want accepted", id, err)
		}
	}
	for _, id := range invalid {
		if _, err := auth.NewStatic(staticIdentityWithTenant(id)); err == nil {
			t.Errorf("auth.NewStatic(%q) accepted, want refused", id)
		}
	}

	// Layer 2: configuration load (internal/config, upstream ids).
	for _, id := range valid {
		setEnv(t, append(baseEnv("127.0.0.1:0"), upstreamsWithID(id)))
		if _, err := config.Load(); err != nil {
			t.Errorf("config.Load with upstream id %q: %v, want accepted", id, err)
		}
	}
	for _, id := range invalid {
		setEnv(t, append(baseEnv("127.0.0.1:0"), upstreamsWithID(id)))
		if _, err := config.Load(); err == nil {
			t.Errorf("config.Load with upstream id %q accepted, want refused", id)
		}
	}

	// Layer 3: the admin quota surface's tenant path segment. A valid
	// shape reaches the ledger and comes back 404 tenant_unknown (the
	// test ledger provisions nobody); an invalid shape is refused at
	// the boundary with 400, before the ledger is consulted. The empty
	// id is absent here: the surface refuses it earlier still, as an
	// empty segment (404), not as a bad character set — the other two
	// layers pin its rejection. The env the loops above left behind is
	// restored to a loadable baseline first: buildAdmin resolves its
	// config through config.Load.
	setEnv(t, baseEnv("127.0.0.1:0"))
	admin := buildAdmin(testConfig("127.0.0.1:0"), adminBindings{
		gov:         &governanceBackends{ledger: quota.NewMemory()},
		metrics:     obs.NewMetrics(),
		upstreamIDs: []string{"u1"},
	})
	for _, id := range valid {
		rec := httptest.NewRecorder()
		admin.ServeHTTP(rec, quotaGet(id))
		if rec.Code != http.StatusNotFound {
			t.Errorf("admin quota GET for %q = %d, want 404 (accepted shape, unprovisioned tenant)", id, rec.Code)
		}
	}
	for _, id := range invalidCharset() {
		rec := httptest.NewRecorder()
		admin.ServeHTTP(rec, quotaGet(id))
		if rec.Code != http.StatusBadRequest {
			t.Errorf("admin quota GET for %q = %d, want 400 (refused shape)", id, rec.Code)
		}
	}
}

// invalidCharset is the out-of-alphabet corpus: every id refused for
// its characters, the empty string excluded (refused as an absent id
// everywhere, before the character rule applies).
func invalidCharset() []string {
	return []string{
		strings.Repeat("a", 129),
		"a b",
		"a/b",
		"a~b",
		"a+b",
		"租户",
	}
}

// quotaGet builds the admin quota read for one tenant id, bearing the
// baseline admin token baseEnv configures.
func quotaGet(id string) *http.Request {
	req := httptest.NewRequest(http.MethodGet,
		"/admin/tenants/"+url.PathEscape(id)+"/quota", nil)
	req.Header.Set("Authorization", "Bearer test-admin")
	return req
}

// staticIdentityWithTenant builds the minimal static identity whose
// single tenant carries the id under test.
func staticIdentityWithTenant(id string) auth.StaticConfig {
	return auth.StaticConfig{
		Tiers: []auth.StaticTier{{
			ID: "free", RPM: 10, TPM: 1000, MaxTokens: 64,
			MonthlyQuota: 1000, AllowedModels: []string{"*"},
		}},
		Tenants: []auth.StaticTenant{{
			ID: id, Name: "T", Tier: "free", Keys: []string{"k"},
		}},
	}
}

// upstreamsWithID is the env entry naming one upstream with the id
// under test (appended after baseEnv, so it wins).
func upstreamsWithID(id string) string {
	return `BREAKWATER_UPSTREAMS=[{"id":` + strconv.Quote(id) +
		`,"base_url":"http://127.0.0.1:1","models":["*"]}]`
}
