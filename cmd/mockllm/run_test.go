/**
 * @file run_test
 * @description Lifecycle tests of the mock upstream binary's run
 * wrapper: a cancelled context ends the server cleanly and a busy port
 * surfaces as a returned error.
 */
package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestRunStopsOnCancelledContext(t *testing.T) {
	// Reserve an ephemeral port for the mock: the address is known up
	// front so readiness can be polled instead of guessed.
	reserved, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("bind: %v", err)
	}
	addr := reserved.Addr().String()
	if err := reserved.Close(); err != nil {
		t.Fatalf("release the port: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx, slog.New(slog.DiscardHandler), options{addr: addr}) }()

	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := http.Get("http://" + addr + "/")
		if err == nil {
			_ = resp.Body.Close()
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("mock upstream never came up on %s: %v", addr, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("run: %v", err)
	}
}

func TestRunSurfacesBusyPort(t *testing.T) {
	blocker := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}))
	defer blocker.Close()

	err := run(context.Background(), slog.New(slog.DiscardHandler),
		options{addr: blocker.Listener.Addr().String()})
	if err == nil {
		t.Fatal("a busy port must surface as a returned error")
	}
}

// TestMainExitsNonZeroOnBusyPort drives the process entry point as a
// subprocess: a busy listen address must turn into a non-zero exit.
func TestMainExitsNonZeroOnBusyPort(t *testing.T) {
	if os.Getenv("BE_MOCKLLM_MAIN") == "1" {
		os.Args = []string{"mockllm", "-addr", os.Getenv("BE_MOCKLLM_ADDR")}
		main()
		return
	}

	blocker := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {}))
	defer blocker.Close()

	// nosemgrep: go.lang.security.audit.dangerous-exec-command.dangerous-exec-command, go_subproc_rule-subproc -- the test binary re-executes itself with a fixed literal argument list
	cmd := exec.Command(os.Args[0], "-test.run=TestMainExitsNonZeroOnBusyPort")
	cmd.Env = append(os.Environ(),
		"BE_MOCKLLM_MAIN=1",
		"BE_MOCKLLM_ADDR="+blocker.Listener.Addr().String(),
	)
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatal("a busy port must produce a non-zero exit")
	}
	if !strings.Contains(string(out), "mock upstream terminated") {
		t.Fatalf("output = %s, want the termination log line", out)
	}
}
