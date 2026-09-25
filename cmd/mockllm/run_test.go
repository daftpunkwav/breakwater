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
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestRunStopsOnCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(100 * time.Millisecond) // let the listener come up
		cancel()
	}()
	if err := run(ctx, slog.New(slog.DiscardHandler), options{addr: "127.0.0.1:0"}); err != nil {
		t.Fatalf("run: %v", err)
	}
}

func TestRunSurfacesBusyPort(t *testing.T) {
	blocker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
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

	blocker := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {}))
	defer blocker.Close()

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
