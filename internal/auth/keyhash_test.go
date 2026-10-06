/**
 * @file keyhash_test
 * @description The API-key lookup hash's contract: HMAC-SHA256 keyed by
 * the deployment pepper, proven against the RFC 4231 vectors and pinned
 * to the digests deploy/seed.sql actually inserts — the one guard
 * against the Go and SQL sides drifting into formats that cannot
 * resolve each other.
 */
package auth

import (
	"os"
	"regexp"
	"testing"
)

// TestHashKeyMatchesRFC4231 pins the construction to real HMAC-SHA256:
// a lookalike digest (say sha256(pepper+raw)) fails these vectors.
func TestHashKeyMatchesRFC4231(t *testing.T) {
	if got := hashKey("Jefe", "what do ya want for nothing?"); got !=
		"5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843" {
		t.Fatalf("hashKey(Jefe, ...) = %s, want the RFC 4231 test case 2 vector", got)
	}

	key := string([]byte{
		0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b,
		0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b,
		0x0b, 0x0b, 0x0b, 0x0b,
	})
	if got := hashKey(key, "Hi There"); got !=
		"b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7" {
		t.Fatalf("hashKey(0x0b*20, ...) = %s, want the RFC 4231 test case 1 vector", got)
	}
}

func TestHashKeyPepperKeysTheDigest(t *testing.T) {
	const raw = "bw-some-key"
	plain := hashKey("", raw)
	peppered := hashKey("deployment-pepper", raw)

	if plain == peppered {
		t.Fatal("a pepper must change the digest")
	}
	if hashKey("deployment-pepper", raw) != peppered {
		t.Fatal("the same pepper must hash deterministically")
	}
	if hashKey("", raw) != plain {
		t.Fatal("the empty pepper must stay stable across calls")
	}
}

// TestSeedKeyHashMatchesHashKey reads the digests straight out of
// deploy/seed.sql — literals duplicated here would let the SQL side
// drift while this test stayed green.
func TestSeedKeyHashMatchesHashKey(t *testing.T) {
	// Literal path: the repo layout is fixed, and os / go test accept
	// forward slashes on Windows too.
	sqlBytes, err := os.ReadFile("../../deploy/seed.sql")
	if err != nil {
		t.Fatalf("read seed.sql: %v", err)
	}
	// Scope the search to the INSERT INTO api_keys statement so a
	// matching tuple left behind by a retargeted insert cannot keep the
	// test green while no api_keys row is seeded.
	stmt := regexp.MustCompile(`(?s)INSERT INTO api_keys\b.*?;`)
	seeded := stmt.Find(sqlBytes)
	if seeded == nil {
		t.Fatal("seed.sql: no INSERT INTO api_keys statement found")
	}
	cases := []struct{ id, raw string }{
		{"key-local-1", "bw-local-t1"},
		{"key-local-2", "bw-local-t2"},
	}
	for _, tc := range cases {
		// Each seed row: ('<id>', '<tenant>', '<64-hex digest>'),
		// possibly wrapped across lines. The digests are the
		// empty-pepper form the seed assumes.
		re := regexp.MustCompile(`'` + tc.id + `',\s*'[^']*',\s*'([0-9a-f]{64})'`)
		m := re.FindSubmatch(seeded)
		if m == nil {
			t.Fatalf("seed.sql: no 64-hex key_hash digest found for %s", tc.id)
		}
		if got := hashKey("", tc.raw); string(m[1]) != got {
			t.Errorf("seed.sql %s digest = %s, want hashKey(\"\", %q) = %s", tc.id, m[1], tc.raw, got)
		}
	}
}
