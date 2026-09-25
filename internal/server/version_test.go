/**
 * @file version_test
 * @description The version endpoint: the build identifier and the
 * running Go version, rendered as JSON.
 */
package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
)

func TestVersionEndpointRendersBuildIdentity(t *testing.T) {
	t.Parallel()
	handler := makeVersion("v0.1-test")
	req := httptest.NewRequest(http.MethodGet, "/version", nil)
	rec := httptest.NewRecorder()
	handler(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	var body struct {
		Version string `json:"version"`
		Go      string `json:"go"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("body = %s err = %v", rec.Body.String(), err)
	}
	if body.Version != "v0.1-test" {
		t.Fatalf("version = %q, want v0.1-test", body.Version)
	}
	if body.Go != runtime.Version() {
		t.Fatalf("go = %q, want %q", body.Go, runtime.Version())
	}
}
