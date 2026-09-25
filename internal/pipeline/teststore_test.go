/**
 * @file teststore_test
 * @description Shared test fixtures for the pipeline stage tests: a
 * scripted auth.Store and a recording obs.Sink.
 */
package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/daftpunkwav/breakwater/internal/auth"
	"github.com/daftpunkwav/breakwater/internal/obs"
	"github.com/daftpunkwav/breakwater/internal/protocol"
)

// stubStore is a scripted auth.Store: one fixed outcome per resolve.
type stubStore struct {
	tenant auth.Tenant
	err    error
}

// Resolve implements auth.Store.
func (s *stubStore) Resolve(_ context.Context, _ string) (auth.Tenant, error) {
	return s.tenant, s.err
}

// errStore is an auth.Store whose error is compared with errors.Is by
// the stage under test (the transient-failure shape).
var errStoreDown = errors.New("identity store down")

// recordingSink is an obs.Sink collecting the entries it was handed.
type recordingSink struct {
	mu      sync.Mutex
	entries []obs.Entry
}

// Record implements obs.Sink.
func (s *recordingSink) Record(entry obs.Entry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.entries = append(s.entries, entry)
}

// Flush implements obs.Sink; the shutdown-path only method.
func (s *recordingSink) Flush(_ context.Context) error { return nil }

// snapshot returns the recorded entries.
func (s *recordingSink) snapshot() []obs.Entry {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]obs.Entry(nil), s.entries...)
}

// assertEnvelope asserts that the recorder holds a canonical OpenAI
// error envelope with the given status and code — the shape every
// gateway rejection renders in by default.
func assertEnvelope(t *testing.T, rec *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	if rec.Code != status {
		t.Fatalf("status = %d, want %d (body: %s)", rec.Code, status, rec.Body.String())
	}
	var envelope protocol.ErrorEnvelope
	if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
		t.Fatalf("body %q is not an error envelope: %v", rec.Body.String(), err)
	}
	if envelope.Error.Code != code {
		t.Fatalf("error code = %q, want %q (body: %s)", envelope.Error.Code, code, rec.Body.String())
	}
}
