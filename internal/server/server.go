/**
 * @file server
 * @description HTTP server lifecycle for the gateway.
 *
 * Responsibilities:
 * - Own the http.Server instance
 * - Serve until the caller's context is cancelled
 * - Drain in-flight requests within a grace deadline (invariant I8)
 *
 * This package must not depend on governance modules; handler wiring
 * happens here as routes, upstream composition in cmd/breakwater.
 */
package server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"
)

// Options configures the server lifecycle.
type Options struct {
	// Addr is the listen address.
	Addr string
	// ShutdownGrace bounds the drain window after shutdown begins.
	ShutdownGrace time.Duration
}

// Server runs the gateway HTTP endpoint.
type Server struct {
	httpServer *http.Server
	grace      time.Duration
}

// New builds a Server with all routes registered.
func New(opts Options) *Server {
	return &Server{
		httpServer: &http.Server{
			Addr:              opts.Addr,
			Handler:           newRootHandler(),
			ReadHeaderTimeout: 10 * time.Second,
		},
		grace: opts.ShutdownGrace,
	}
}

// Run serves until ctx is cancelled, then drains connections within the
// grace deadline. A startup failure (e.g. port already in use) is
// returned as-is.
func (s *Server) Run(ctx context.Context) error {
	listener, err := net.Listen("tcp", s.httpServer.Addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", s.httpServer.Addr, err)
	}

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- s.httpServer.Serve(listener)
	}()

	select {
	case err := <-serveErr:
		return err
	case <-ctx.Done():
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), s.grace)
	defer cancel()
	if err := s.httpServer.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("drain connections: %w", err)
	}
	return nil
}
