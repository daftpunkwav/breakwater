/**
 * @file main
 * @description Composition root of the breakwater gateway binary.
 *
 * Responsibilities:
 * - Bridge the process boundary: signals, exit codes, the root logger
 * - Delegate every assembly and lifecycle decision to run
 *
 * This file stays thin on purpose: everything after the process
 * boundary lives in run, which returns errors instead of exiting and
 * is therefore testable end to end.
 */
package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/daftpunkwav/breakwater/internal/config"
)

// version is injected at build time (Makefile: -ldflags -X ...).
var version = "dev"

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	if err := run(context.Background(), logger, version); err != nil {
		logger.Error("gateway terminated", "error", err)
		os.Exit(1)
	}
}

// run assembles and serves the gateway until the process is signalled
// or a startup step fails; configuration errors and assembly failures
// surface as returned errors.
func run(ctx context.Context, logger *slog.Logger, version string) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	return serve(ctx, cfg, logger, version)
}
