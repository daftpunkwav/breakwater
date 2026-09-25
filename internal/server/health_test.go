/**
 * @file health_test
 * @description Probe tests: liveness answers, and readiness reflecting
 * the injected dependency probe (error -> 503, nil probe -> ok).
 */
package server

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestLivenessAnswersOK(t *testing.T) {
	t.Parallel()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rec := httptest.NewRecorder()
	handleLiveness(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !strings.HasPrefix(rec.Body.String(), "ok") {
		t.Fatalf("body = %q, want ok", rec.Body.String())
	}
}

func TestReadinessReflectsInjectedProbe(t *testing.T) {
	t.Parallel()
	// A failing dependency flips readiness to 503 with the reason.
	failing := makeReadiness(func() error { return errors.New("redis: connection refused") })
	rec := httptest.NewRecorder()
	failing(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503 for a failing probe", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "not ready: redis: connection refused") {
		t.Fatalf("body = %q, want the probe error verbatim", rec.Body.String())
	}

	// A healthy dependency reports ready.
	healthy := makeReadiness(func() error { return nil })
	rec = httptest.NewRecorder()
	healthy(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 for a passing probe", rec.Code)
	}

	// No probe injected: always ready.
	open := makeReadiness(nil)
	rec = httptest.NewRecorder()
	open(rec, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 without a probe", rec.Code)
	}
}
