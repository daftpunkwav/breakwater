/**
 * @file httpserver
 * @description Neutral HTTP server lifecycle shared by every binary in
 * this repository: bind, serve until the caller's context is cancelled,
 * drain in-flight requests within a grace deadline (invariant I8).
 *
 * Responsibilities:
 * - Own the http.Server lifecycle, exactly once across binaries
 * - Nothing else: routing, handlers, configuration and governance live
 *   with their owners; this package imports only the standard library
 */
package httpserver

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"
)

// Options configures one server run.
type Options struct {
	// Addr is the listen address.
	Addr string
	// Handler is the root handler to serve.
	Handler http.Handler
	// ShutdownGrace bounds the drain window after shutdown begins.
	ShutdownGrace time.Duration
}

// Run serves opts.Handler on opts.Addr until ctx is cancelled, then
// drains connections within opts.ShutdownGrace. A startup failure (e.g.
// port already in use) is returned as-is.
func Run(ctx context.Context, opts Options) error {
	if opts.Handler == nil {
		return fmt.Errorf("httpserver: no handler configured")
	}
	if opts.ShutdownGrace <= 0 {
		opts.ShutdownGrace = 10 * time.Second
	}

	listener, err := net.Listen("tcp", opts.Addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", opts.Addr, err)
	}

	httpSrv := &http.Server{
		Handler:           opts.Handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- httpSrv.Serve(listener)
	}()

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), opts.ShutdownGrace)
	defer cancel()
	if err := httpSrv.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("drain connections: %w", err)
	}
	return nil
}
