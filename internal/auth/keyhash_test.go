/**
 * @file keyhash_test
 * @description The API-key lookup hash's contract: HMAC-SHA256 keyed by
 * the deployment pepper, with the empty-pepper digests pinned to the
 * values deploy/seed.sql persists — the one guard against the Go and
 * SQL sides drifting into formats that cannot resolve each other.
 */
package auth

import "testing"

func TestSeedKeyHashMatchesHashKey(t *testing.T) {
	// deploy/seed.sql inserts these digests for the loadtest keys,
	// assuming BREAKWATER_KEY_PEPPER is unset at seed time.
	cases := []struct {
		raw  string
		want string
	}{
		{"bw-local-t1", "dbd756faa78508795443155672311944720bab34224dcdde87e6a1bfb2838088"},
		{"bw-local-t2", "a544711afeb45d9c00135b1c18c3e05582f59bd78272137fd30fc204aed2fd76"},
	}
	for _, tc := range cases {
		if got := hashKey("", tc.raw); got != tc.want {
			t.Errorf("hashKey(\"\", %q) = %s, want the seed.sql digest %s", tc.raw, got, tc.want)
		}
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
