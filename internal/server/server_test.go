/**
 * @file server_test
 * @description The gateway server facade: New assembles the options and
 * Run serves the assembled root handler — including readiness gating on
 * the injected probe — until cancellation, then returns cleanly.
 */
package server

import (
	"context"
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

func TestServerRunServesUntilCancel(t *testing.T) {
	t.Parallel()
	ready := false
	listener := testListener(t)
	srv := New(Options{
		Addr:     listener.Addr().String(),
		Listener: listener,
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

	// Poll until the listener is up, then check both probes.
	url := "http://" + listener.Addr().String()
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := http.Get(url + "/healthz")
		if err == nil {
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

	resp, err := http.Get(url + "/readyz")
	if err != nil {
		t.Fatalf("readyz: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("readyz status = %d, want 503 while the probe fails", resp.StatusCode)
	}

	ready = true
	resp, err = http.Get(url + "/readyz")
	if err != nil {
		t.Fatalf("readyz: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("readyz status = %d, want 200 once the probe passes", resp.StatusCode)
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

// errNotReady is the injected probe failure of the run test.
type errNotReady struct{}

func (errNotReady) Error() string { return "dependencies warming up" }
