/**
 * @file httpserver
 * @description Neutral HTTP transport mechanics shared by every binary
 * in this repository: the serve/drain lifecycle and
 * stdlib-only transport helpers (e.g. the response tee used by
 * forwarding).
 *
 * Responsibilities:
 * - Own the http.Server lifecycle, exactly once across binaries
 * - Host neutral, business-agnostic transport utilities
 * - Nothing else: routing, handlers, configuration and governance live
 *   with their owners; this package imports only the standard library
 */
package httpserver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"
)

// Options configures one server run.
type Options struct {
	// Addr is the listen address. Ignored when Listener is set.
	Addr string
	// Handler is the root handler to serve.
	Handler http.Handler
	// ShutdownGrace bounds the drain window after shutdown begins.
	ShutdownGrace time.Duration
	// Listener, when set, is served instead of binding Addr. A caller
	// that already holds a bound listener passes it here, which removes
	// the reserve-release-rebind race a caller would otherwise have to
	// accept when it needs a known address up front. Ownership transfers
	// with it: Run closes the listener when serving ends (Serve closes
	// it on every return path), so the caller must neither close nor
	// reuse it afterwards.
	Listener net.Listener
}

// ErrDrainTimeout reports that the drain window expired with requests
// still in flight. That is a shutdown with a warning, not a failure:
// callers exit zero on it and log instead. Other Shutdown errors
// (genuine transport breakdowns) surface wrapped as before.
var ErrDrainTimeout = errors.New("httpserver: drain window expired with requests in flight")

// Run serves opts.Handler on opts.Addr until ctx is cancelled, then
// drains connections within opts.ShutdownGrace. A startup failure (e.g.
// port already in use) is returned as-is; a drain that outlives the
// grace window returns ErrDrainTimeout.
func Run(ctx context.Context, opts Options) error {
	if opts.Handler == nil {
		return fmt.Errorf("httpserver: no handler configured")
	}
	if opts.ShutdownGrace <= 0 {
		opts.ShutdownGrace = 10 * time.Second
	}

	listener := opts.Listener
	if listener == nil {
		var err error
		listener, err = net.Listen("tcp", opts.Addr)
		if err != nil {
			return fmt.Errorf("listen on %s: %w", opts.Addr, err)
		}
	}

	httpSrv := &http.Server{
		Handler:           opts.Handler,
		ReadHeaderTimeout: 10 * time.Second,
		// ReadTimeout bounds reading one request (headers plus body) —
		// with the 4 MiB body cap, tens of seconds are generous, and a
		// slowloris body must not hold a worker forever. It never
		// touches the response side, so SSE streams are unaffected.
		// IdleTimeout reaps keep-alive connections whose client vanished
		// without a FIN; it never bounds an in-flight request either.
		// WriteTimeout stays unset on purpose: it would kill long
		// legitimate streams.
		ReadTimeout: 30 * time.Second,
		IdleTimeout: 120 * time.Second,
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
		if errors.Is(err, context.DeadlineExceeded) {
			return ErrDrainTimeout
		}
		return fmt.Errorf("drain connections: %w", err)
	}
	return nil
}
