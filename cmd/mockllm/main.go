/**
 * @file main
 * @description Composition root of the mockllm binary: a mock
 * OpenAI-compatible upstream serving as the fault injector for gateway
 * experiments.
 *
 * Responsibilities:
 * - Bridge the process boundary: flags, signals, exit codes
 * - Delegate the assembly and lifecycle to run
 *
 * The file stays thin: run returns errors instead of exiting, so the
 * lifecycle is testable.
 */
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/daftpunkwav/breakwater/internal/httpserver"
	"github.com/daftpunkwav/breakwater/internal/mockllm"
)

// options carries the parsed process flags.
type options struct {
	addr         string
	defaultDelay time.Duration
	errorRate    float64
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	var opts options
	flag.StringVar(&opts.addr, "addr", ":8090", "listen address")
	flag.DurationVar(&opts.defaultDelay, "default-delay", 0, "delay injected into every request")
	flag.Float64Var(&opts.errorRate, "error-rate", 0, "fraction of requests answered with 500 (0..1)")
	flag.Parse()

	if err := run(context.Background(), logger, opts); err != nil {
		logger.Error("mock upstream terminated", "error", err)
		os.Exit(1)
	}
}

// run serves the mock upstream until ctx or a process signal ends it.
func run(ctx context.Context, logger *slog.Logger, opts options) error {
	ctx, stop := context.WithCancel(ctx)
	defer stop()
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	go func() {
		select {
		case <-sigs:
		case <-ctx.Done():
		}
		stop()
	}()

	handler := mockllm.New(mockllm.Options{
		DefaultDelay: opts.defaultDelay,
		ErrorRate:    opts.errorRate,
	})
	logger.Info("mock upstream starting", "addr", opts.addr)
	return httpserver.Run(ctx, httpserver.Options{
		Addr:          opts.addr,
		Handler:       handler,
		ShutdownGrace: 10 * time.Second,
	})
}
