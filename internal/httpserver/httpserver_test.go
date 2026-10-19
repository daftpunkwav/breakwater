/**
 * @file httpserver_test
 * @description Lifecycle tests: serving, draining after cancel, and
 * surfacing startup failures.
 */
package httpserver

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

// freePort reserves an ephemeral port and releases it immediately; the
// small reuse race is acceptable for tests.
func freePort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	addr := l.Addr().String()
	if err := l.Close(); err != nil {
		t.Fatalf("release port: %v", err)
	}
	return addr
}

// runInBackground starts Run on its own goroutine over a fresh
// cancellable context and returns the cancel and Run's result channel.
func runInBackground(opts Options) (context.CancelFunc, <-chan error) {
	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() {
		runErr <- Run(ctx, opts)
	}()
	return cancel, runErr
}

// awaitReady GETs the URL until it answers or the deadline expires; it
// returns the first successful response (caller closes the body) or
// the last dial error.
func awaitReady(client *http.Client, url string, timeout time.Duration) (*http.Response, error) {
	deadline := time.Now().Add(timeout)
	var err error
	for {
		var resp *http.Response
		resp, err = client.Get(url)
		if err == nil {
			return resp, nil
		}
		if time.Now().After(deadline) {
			return nil, err
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// drainGrace is the shutdown window the drain tests grant the server:
// long enough for a real drain, short enough that a drain which
// overruns it surfaces as an error instead of a hang.
const drainGrace = 2 * time.Second

func TestRunServesAndDrains(t *testing.T) {
	t.Parallel()
	addr := freePort(t)

	cancel, runErr := runInBackground(Options{
		Addr:          addr,
		Handler:       http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }),
		ShutdownGrace: drainGrace,
	})
	defer cancel()

	// The client keeps a transport of its own: the process-wide default
	// transport shares one connection pool across the whole test
	// binary, and a keep-alive connection left behind by another test
	// would show up in this server's drain.
	transport := &http.Transport{}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}

	resp, err := awaitReady(client, "http://"+addr+"/", 5*time.Second)
	if err != nil {
		t.Fatalf("server never became ready: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	// The body is drained before it is closed: a body closed unread
	// leaves the connection half-closed, and the client transport then
	// dials a replacement the server holds in StateNew — a connection
	// the drain waits on for seconds.
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()

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

func TestRunSurfacesStartupFailure(t *testing.T) {
	t.Parallel()
	// Occupy a port, then ask Run for the same one.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer func() { _ = l.Close() }()

	err = Run(context.Background(), Options{Addr: l.Addr().String(), Handler: http.NotFoundHandler()})
	if err == nil {
		t.Fatal("expected startup failure for occupied port")
	}
}

// TestRunRejectsMissingHandler pins the startup guard: a nil handler is
// a configuration error surfaced before any listener is opened.
func TestRunRejectsMissingHandler(t *testing.T) {
	t.Parallel()
	err := Run(context.Background(), Options{Addr: "127.0.0.1:0"})
	if err == nil {
		t.Fatal("nil handler accepted")
	}
	if got := err.Error(); !strings.Contains(got, "no handler") {
		t.Fatalf("err = %v, want the missing-handler message", err)
	}
}

// TestRunDrainTimeoutSurfacesError pins the drain bound: a handler
// holding its connection open past the shutdown grace makes Run return
// ErrDrainTimeout (a warning, not a startup failure) instead of hanging
// forever.
func TestRunDrainTimeoutSurfacesError(t *testing.T) {
	t.Parallel()
	addr := freePort(t)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) }) // let the stuck handler finish
	started := make(chan struct{})

	handler := http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		close(started)
		<-release
	})

	cancel, runErr := runInBackground(Options{Addr: addr, Handler: handler, ShutdownGrace: 150 * time.Millisecond})

	// Open one in-flight request so Shutdown has a connection to wait
	// for. Ephemeral-port reuse can refuse the first dial on Windows,
	// so dialing retries until the handler is serving.
	go func() {
		if resp, err := awaitReady(http.DefaultClient, "http://"+addr+"/", 10*time.Second); err == nil {
			_ = resp.Body.Close()
		}
	}()

	// Wait for the request to be served, but never unbounded: on a
	// contended runner the client may fail to connect at all.
	select {
	case <-started:
	case err := <-runErr:
		t.Fatalf("run exited before the request started: %v", err)
	case <-time.After(10 * time.Second):
		t.Fatal("handler never started")
	}
	cancel()

	select {
	case err := <-runErr:
		if !errors.Is(err, ErrDrainTimeout) {
			t.Fatalf("err = %v, want ErrDrainTimeout", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return after the drain window")
	}
	// The stuck request goroutine finishes once the drain force-close
	// errors its connection or at cleanup (release closed); it must not
	// block the test's exit path.
}
