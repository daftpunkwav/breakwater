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
	if err := loadWithUpstreams(t,
		`[{"id":"a","base_url":"http://127.0.0.1:8090","models":["*"]}]`); err != nil {
		t.Fatalf("valid upstreams rejected: %v", err)
	}
}
