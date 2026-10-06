/**
 * @file testhelpers_test
 * @description Shared scaffolding of the relay engine tests: canned
 * stub upstreams and replies for the common status shapes, the SSE
 * streaming fixtures, the context-aware pipe wait used by the stream
 * timing tests, and the credential-ring fixture of the rotation tests.
 */
package relay

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/daftpunkwav/breakwater/internal/upstream"
)

// errReader fails every read, simulating a connection reset under the
// relay's body reads.
type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("connection reset by peer") }

// jsonStubUpstream returns a stub upstream whose every exchange
// answers with the given status and JSON body.
func jsonStubUpstream(t *testing.T, id string, status int, body string) *stubUpstream {
	t.Helper()
	return &stubUpstream{id: id, fn: func(context.Context, upstream.Request) (*upstream.Response, error) {
		return jsonResponse(t, status, body), nil
	}}
}

// countedStubUpstream returns a stub upstream that counts every
// exchange into calls and answers each one with the same status and
// JSON body; tests assert on the count after the run.
func countedStubUpstream(t *testing.T, calls *int, id string, status int, body string) *stubUpstream {
	t.Helper()
	return &stubUpstream{id: id, fn: func(context.Context, upstream.Request) (*upstream.Response, error) {
		*calls++
		return jsonResponse(t, status, body), nil
	}}
}

// failingStubUpstream returns a stub upstream whose Forward always
// fails with err — the transport-refused fixture.
func failingStubUpstream(id string, err error) *stubUpstream {
	return &stubUpstream{id: id, fn: func(context.Context, upstream.Request) (*upstream.Response, error) {
		return nil, err
	}}
}

// unreadableStubUpstream returns a stub upstream answering status with
// a JSON-typed body that fails on every read — the connection-reset
// fixture of the read-failure paths.
func unreadableStubUpstream(id string, status int) *stubUpstream {
	return &stubUpstream{id: id, fn: func(context.Context, upstream.Request) (*upstream.Response, error) {
		return stubResponse(status, errReader{}, func(h http.Header) { h.Set("Content-Type", "application/json") }), nil
	}}
}

// stubResponse builds a canned upstream reply over body with status;
// set, when given, adds the headers under test before the commit rules
// run.
func stubResponse(status int, body io.Reader, set func(http.Header)) *upstream.Response {
	header := http.Header{}
	if set != nil {
		set(header)
	}
	return &upstream.Response{StatusCode: status, Header: header, Body: io.NopCloser(body)}
}

// sseResponse builds a committed streaming reply: a 200 with the SSE
// content type over body.
func sseResponse(body io.Reader) *upstream.Response {
	return stubResponse(http.StatusOK, body, func(h http.Header) { h.Set("Content-Type", "text/event-stream") })
}

// waitOrDie waits delay, closing pw with the context's error and
// returning false if ctx is cancelled first — the body-death semantics
// of a real response stream cut mid-read.
func waitOrDie(ctx context.Context, pw *io.PipeWriter, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	select {
	case <-timer.C:
		timer.Stop()
		return true
	case <-ctx.Done():
		timer.Stop()
		pw.CloseWithError(ctx.Err())
		return false
	}
}

// dripStream returns a body reader that emits n data frames with a
// delay between them, then a clean [DONE]. Like a real HTTP response
// body, it fails with the context's error when the context is
// cancelled mid-drip.
func dripStream(ctx context.Context, n int, delay time.Duration) io.Reader {
	pr, pw := io.Pipe()
	go func() {
		for i := 1; i <= n; i++ {
			if !waitOrDie(ctx, pw, delay) {
				return
			}
			if _, err := fmt.Fprintf(pw, "data: {\"choices\":[{\"delta\":{\"content\":\"c%d\"}}]}\n\n", i); err != nil {
				pw.CloseWithError(err)
				return
			}
		}
		_, _ = fmt.Fprint(pw, "data: [DONE]\n\n")
		_ = pw.Close()
	}()
	return pr
}

// sseDripUpstream is one stub upstream answering a streaming 200 whose
// body drips n frames one delay apart — the standard fixture of the
// stream timing tests; the body dies with the request's context.
func sseDripUpstream(n int, delay time.Duration) *stubUpstream {
	return &stubUpstream{id: "s", fn: func(ctx context.Context, _ upstream.Request) (*upstream.Response, error) {
		return sseResponse(dripStream(ctx, n, delay)), nil
	}}
}

// sseUpstream is one stub upstream answering a streaming 200 with a
// fixed body.
func sseUpstream(body io.Reader) *stubUpstream {
	return &stubUpstream{id: "s", fn: func(context.Context, upstream.Request) (*upstream.Response, error) {
		return sseResponse(body), nil
	}}
}

// newUnauthorizedRing builds a two-key ring upstream whose first
// credential ("sk-bad") dies with a credential-class 401 and whose
// second ("sk-good") passes; the key server records every bearer
// credential it saw.
func newUnauthorizedRing(t *testing.T) (*keyServer, *upstream.OpenAI) {
	t.Helper()
	ks := newKeyServer(t, map[string]keyFailure{
		"sk-bad": {status: http.StatusUnauthorized, body: `{"error":{"type":"authentication_error"}}`},
	})
	return ks, newRingUpstream(t, ks.server.URL, "sk-bad", "sk-good")
}
