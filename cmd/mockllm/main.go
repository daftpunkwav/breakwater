/**
 * @file main
 * @description Composition root of the mockllm binary: a mock
 * OpenAI-compatible upstream serving as the fault injector for gateway
 * experiments.
 *
 * Responsibilities:
 * - Parse process flags
 * - Assemble the mock handler and hand it to the shared server lifecycle
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

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	var (
		addr         = flag.String("addr", ":8090", "listen address")
		defaultDelay = flag.Duration("default-delay", 0, "delay injected into every request")
		errorRate    = flag.Float64("error-rate", 0, "fraction of requests answered with 500 (0..1)")
	)
	flag.Parse()

	handler := mockllm.New(mockllm.Options{
		DefaultDelay: *defaultDelay,
		ErrorRate:    *errorRate,
	})

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("mock upstream starting", "addr", *addr)
	if err := httpserver.Run(ctx, httpserver.Options{
		Addr:          *addr,
		Handler:       handler,
		ShutdownGrace: 10 * time.Second,
	}); err != nil {
		logger.Error("mock upstream terminated", "error", err)
		os.Exit(1)
	}
	logger.Info("mock upstream stopped")
}
