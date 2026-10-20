/**
 * @file server_test
 * @description The gateway server facade: New assembles the options and
 * Run serves the assembled root handler — including readiness gating on
 * the injected probe — until cancellation, then returns cleanly.
 */
package server

import (
	"context"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

// testAddr reserves an ephemeral port and releases it immediately; the
// small reuse race is acceptable for tests.
// testListener returns a bound listener for a test server. The listener
// is handed to the server rather than released first: reserving an
// address and rebinding it later is a race with every other process
// picking the same free port, and this suite runs its packages in
// parallel.
func testListener(t *testing.T) net.Listener {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("bind listener: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	return l
}

// testClient returns a client with a transport of its own. The
// process-wide default transport shares one connection pool across the
// whole test binary, so a keep-alive connection left behind by another
// test would show up in this server's drain.
func testClient(t *testing.T) (*http.Client, *http.Transport) {
	t.Helper()
	tr := &http.Transport{}
	t.Cleanup(tr.CloseIdleConnections)
	return &http.Client{Transport: tr}, tr
}

// getStatus GETs the URL and returns the response status. The body is
// drained before it is closed: a body closed unread leaves the
// connection half-closed, and the client transport then dials a
// replacement the server holds in StateNew — a connection the drain
// waits on for seconds.
func getStatus(t *testing.T, client *http.Client, url string) int {
	t.Helper()
	resp, err := client.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	return resp.StatusCode
}

func TestServerRunServesUntilCancel(t *testing.T) {
	t.Parallel()
	ready := false
	listener := testListener(t)
	srv := New(Options{
		Addr:     listener.Addr().String(),
		Listener: listener,
		// The drain window is the server's own budget, so the test
		// states it instead of leaning on the transport default: the
		// wait below has to cover the grace the drain may spend.
		ShutdownGrace: drainGrace,
		Readiness: func() error {
			if ready {
				return nil
			}
			return errNotReady{}
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() { runErr <- srv.Run(ctx) }()

	client, transport := testClient(t)

	// Poll until the listener is up, then check both probes.
	url := "http://" + listener.Addr().String()
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := client.Get(url + "/healthz")
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("healthz status = %d, want 200", resp.StatusCode)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("server never became ready: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	if got := getStatus(t, client, url+"/readyz"); got != http.StatusServiceUnavailable {
		t.Fatalf("readyz status = %d, want 503 while the probe fails", got)
	}

	ready = true
	if got := getStatus(t, client, url+"/readyz"); got != http.StatusOK {
		t.Fatalf("readyz status = %d, want 200 once the probe passes", got)
	}

	// The client's keep-alive connections go before the cancel: the
	// server then drains nothing and returns as soon as it is told to
	// stop, which is what this test is about. A connection the client
	// still holds would leave the drain waiting on a peer that is not
	// going to send anything.
	transport.CloseIdleConnections()
	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(drainGrace + 3*time.Second):
		t.Fatal("run did not return after cancel")
	}
}

// drainGrace is the shutdown window the run test grants the server:
// long enough for a real drain, short enough that a drain which
// overruns it surfaces as an error instead of a hang.
const drainGrace = 2 * time.Second

// errNotReady is the injected probe failure of the run test.
type errNotReady struct{}

func (errNotReady) Error() string { return "dependencies warming up" }
