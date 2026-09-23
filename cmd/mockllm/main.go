/**
 * @file main
 * @description Composition root of the mockllm binary: a mock
 * OpenAI-compatible upstream serving as the fault injector for gateway
 * experiments.
 *
 * Responsibilities:
 * - Parse process flags
 * - Assemble and run the mock HTTP server with graceful shutdown
 */
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

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

	httpServer := &http.Server{
		Addr:              *addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	listener, err := net.Listen("tcp", *addr)
	if err != nil {
		logger.Error("listen failed", "addr", *addr, "error", err)
		os.Exit(1)
	}
	logger.Info("mock upstream listening", "addr", listener.Addr().String())

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- httpServer.Serve(listener)
	}()

	select {
	case err := <-serveErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server failed", "error", err)
			os.Exit(1)
		}
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown failed", "error", err)
		os.Exit(1)
	}
	logger.Info("mock upstream stopped")
}
