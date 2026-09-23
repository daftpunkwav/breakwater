/**
 * @file main
 * @description Composition root of the breakwater gateway binary.
 *
 * Responsibilities:
 * - Load configuration from the environment
 * - Assemble the HTTP server
 * - Own the process lifecycle: signal handling and shutdown ordering
 *
 * Governance pipeline wiring lands with the forwarding milestone; this
 * root stays the only place that knows concrete implementations.
 */
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/daftpunkwav/breakwater/internal/config"
	"github.com/daftpunkwav/breakwater/internal/server"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg, err := config.Load()
	if err != nil {
		logger.Error("load configuration", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	srv := server.New(server.Options{
		Addr:          cfg.Server.Addr,
		ShutdownGrace: cfg.Server.ShutdownGrace,
	})

	logger.Info("gateway starting", "addr", cfg.Server.Addr)
	if err := srv.Run(ctx); err != nil {
		logger.Error("server terminated", "error", err)
		os.Exit(1)
	}
	logger.Info("gateway stopped")
}
