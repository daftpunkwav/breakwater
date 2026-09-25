/**
 * @file testupstream_test
 * @description Shared fixtures of the relay engine tests: the stub
 * upstream, the JSON response builder, the default test policy and the
 * execute helper that captures what reached the client.
 */
package relay

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/daftpunkwav/breakwater/internal/retry"
	"github.com/daftpunkwav/breakwater/internal/upstream"
)

// stubUpstream answers Forward from a function.
type stubUpstream struct {
	id string
	fn func(ctx context.Context, req upstream.Request) (*upstream.Response, error)
}

func (s *stubUpstream) ID() string { return s.id }

func (s *stubUpstream) Forward(ctx context.Context, req upstream.Request) (*upstream.Response, error) {
	return s.fn(ctx, req)
}

func (s *stubUpstream) Probe(context.Context) error { return nil }

func jsonResponse(t *testing.T, status int, body string) *upstream.Response {
	t.Helper()
	header := http.Header{}
	header.Set("Content-Type", "application/json")
	return &upstream.Response{
		StatusCode: status,
		Header:     header,
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}

func testPolicy() retry.Policy {
	return retry.Policy{MaxAttempts: 3}
}

// capturedResult carries the Result plus what reached the client.
type capturedResult struct {
	Result
	Body   []byte
	Header http.Header
}

func execute(t *testing.T, exec *Executor, candidates []upstream.Upstream, stream bool, body string) capturedResult {
	t.Helper()
	rec := httptest.NewRecorder()
	result := exec.Execute(context.Background(), Job{
		Model:      "test-model",
		Stream:     stream,
		Body:       []byte(body),
		Candidates: candidates,
		Out:        rec,
	})
	return capturedResult{Result: result, Body: rec.Body.Bytes(), Header: rec.Header()}
}
