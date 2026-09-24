/**
 * @file routes_test
 * @description Route surface tests: probes answer and unknown paths 404.
 */
package server

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRootHandlerProbes(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(newRootHandler(nil))
	t.Cleanup(srv.Close)

	for _, path := range []string{"/healthz", "/readyz"} {
		resp, err := http.Get(srv.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("GET %s status = %d, want 200", path, resp.StatusCode)
		}
	}
}

func TestRootHandlerUnknownPath(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(newRootHandler(nil))
	t.Cleanup(srv.Close)

	resp, err := http.Get(srv.URL + "/nope")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}
