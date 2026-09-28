/**
 * @file deployment_guard_test
 * @description The unauthenticated-deployment refusal: an upstream-armed
 * gateway without an identity source, and a PostgreSQL identity without
 * an admin token, fail to boot unless the operator opts in explicitly;
 * the identity source may also load from a file:// URL.
 */
package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestLoadRejectsUpstreamsWithoutIdentity: upstreams without any
// identity source are an unauthenticated open proxy to the configured
// credentials and must refuse to boot, naming both remedies.
func TestLoadRejectsUpstreamsWithoutIdentity(t *testing.T) {
	cleanEnv(t)
	t.Setenv(envUpstreams, `[{"id":"a","base_url":"http://127.0.0.1:8090","models":["*"]}]`)

	_, err := Load()
	if err == nil {
		t.Fatal("Load accepted upstreams without an identity source")
	}
	for _, want := range []string{envUpstreams, envIdentity, envPostgresDSN, envAllowUnauthenticated} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error = %q, want it to name %s", err, want)
		}
	}
}

// TestLoadRejectsPostgresIdentityWithoutAdminToken: a PostgreSQL
// identity without an admin token leaves the key-minting surface open.
func TestLoadRejectsPostgresIdentityWithoutAdminToken(t *testing.T) {
	cleanEnv(t)
	t.Setenv(envPostgresDSN, "postgres://db.local/bw")

	_, err := Load()
	if err == nil {
		t.Fatal("Load accepted a PostgreSQL identity without an admin token")
	}
	for _, want := range []string{envPostgresDSN, envAdminToken, envAllowUnauthenticated} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error = %q, want it to name %s", err, want)
		}
	}
}

// TestLoadAcceptsStaticIdentityWithoutAdminToken: the documented
// quick-start posture — upstreams armed with the static identity set and
// no admin token — still boots. The static mode has no key-minting
// surface, so the open admin area there is the documented dev trade-off.
func TestLoadAcceptsStaticIdentityWithoutAdminToken(t *testing.T) {
	cleanEnv(t)
	t.Setenv(envUpstreams, `[{"id":"a","base_url":"http://127.0.0.1:8090","models":["*"]}]`)
	t.Setenv(envIdentity, `{"tiers":[{"id":"free","models":["*"]}],"tenants":[{"id":"t","tier":"free","keys":["k"]}]}`)

	if _, err := Load(); err != nil {
		t.Fatalf("Load rejected the static-identity quick start: %v", err)
	}
}

// TestLoadOptOutArmsTheUnauthenticatedPosture: the explicit opt-in
// permits both refused postures, and a malformed boolean is still
// rejected.
func TestLoadOptOutArmsTheUnauthenticatedPosture(t *testing.T) {
	cleanEnv(t)
	t.Setenv(envUpstreams, `[{"id":"a","base_url":"http://127.0.0.1:8090","models":["*"]}]`)
	t.Setenv(envAllowUnauthenticated, "true")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load rejected the explicit opt-in: %v", err)
	}
	if len(cfg.Upstreams) != 1 {
		t.Fatalf("upstreams = %+v, want the configured entry", cfg.Upstreams)
	}

	cleanEnv(t)
	t.Setenv(envAllowUnauthenticated, "perhaps")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "parse") {
		t.Fatalf("err = %v, want a parse rejection for the opt-in boolean", err)
	}
}

// TestLoadIdentityFromFile: the file:// form loads the identity JSON
// from disk; an unreadable or unnamed file refuses to boot.
func TestLoadIdentityFromFile(t *testing.T) {
	cleanEnv(t)
	path := filepath.Join(t.TempDir(), "identity.json")
	identity := `{"tiers":[{"id":"free","models":["*"]}],"tenants":[{"id":"t","tier":"free","keys":["k"]}]}`
	if err := os.WriteFile(path, []byte(identity), 0o600); err != nil {
		t.Fatalf("write identity file: %v", err)
	}
	t.Setenv(envIdentity, "file://"+path)

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load with a file:// identity: %v", err)
	}
	if cfg.Identity != identity {
		t.Fatalf("identity = %q, want the file content", cfg.Identity)
	}

	cleanEnv(t)
	t.Setenv(envIdentity, "file://"+filepath.Join(t.TempDir(), "missing.json"))
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), envIdentity) {
		t.Fatalf("err = %v, want a named read failure for the missing identity file", err)
	}

	cleanEnv(t)
	t.Setenv(envIdentity, "file://")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), envIdentity) {
		t.Fatalf("err = %v, want a rejection for the empty file:// identity", err)
	}
}
