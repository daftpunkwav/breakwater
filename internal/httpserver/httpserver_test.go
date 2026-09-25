/**
 * @file httpserver_test
 * @description Lifecycle tests: serving, draining after cancel, and
 * surfacing startup failures.
 */
package httpserver

import (
	"context"
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

func TestRunServesAndDrains(t *testing.T) {
	t.Parallel()
	addr := freePort(t)

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() {
		runErr <- Run(ctx, Options{
			Addr:    addr,
			Handler: http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }),
		})
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := http.Get("http://" + addr + "/")
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("status = %d, want 200", resp.StatusCode)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("server never became ready: %v", err)
		}
		time.Sleep(20 * time.Millisecond)
	}

	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Fatalf("run returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
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
// the drain failure instead of hanging forever.
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

	ctx, cancel := context.WithCancel(context.Background())
	runErr := make(chan error, 1)
	go func() {
		runErr <- Run(ctx, Options{Addr: addr, Handler: handler, ShutdownGrace: 150 * time.Millisecond})
	}()

	// Open one in-flight request so Shutdown has a connection to wait for.
	go func() {
		client := &http.Client{Timeout: 5 * time.Second}
		resp, err := client.Get("http://" + addr + "/")
		if err == nil {
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
		if err == nil {
			t.Fatal("drain timeout not reported")
		}
		if !strings.Contains(err.Error(), "drain connections") {
			t.Fatalf("err = %v, want the drain failure", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("run did not return after the drain window")
	}
	// The stuck request goroutine finishes on the client timeout or at
	// cleanup (release closed); it must not block the test's exit path.
}
