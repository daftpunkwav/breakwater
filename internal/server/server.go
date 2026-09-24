/**
 * @file server
 * @description Gateway HTTP server facade: owns the route assembly and
 * delegates the process lifecycle to the neutral httpserver package.
 *
 * Responsibilities:
 * - Assemble the gateway's root handler (see routes.go)
 * - Nothing else: the listen/serve/drain lifecycle lives in
 *   internal/httpserver so every binary shares one shutdown ordering
 *   (invariant I8)
 */
package server

import (
	"context"
	"net/http"
	"time"

	"github.com/daftpunkwav/breakwater/internal/httpserver"
)

// Options configures the gateway server.
type Options struct {
	// Addr is the listen address.
	Addr string
	// ShutdownGrace bounds the drain window after shutdown begins.
	ShutdownGrace time.Duration
	// Completions is the /v1/chat/completions handler; nil leaves the
	// business route unregistered.
	Completions http.Handler
	// Readiness reports whether business traffic may be served; nil
	// always reports ready.
	Readiness func() error
}

// Server runs the gateway HTTP endpoint.
type Server struct {
	opts Options
}

// New builds a Server with all routes registered.
func New(opts Options) *Server {
	return &Server{opts: opts}
}

// Run serves until ctx is cancelled, then drains connections. A startup
// failure (e.g. port already in use) is returned as-is.
func (s *Server) Run(ctx context.Context) error {
	return httpserver.Run(ctx, httpserver.Options{
		Addr:          s.opts.Addr,
		Handler:       newRootHandler(s.opts.Completions, s.opts.Readiness),
		ShutdownGrace: s.opts.ShutdownGrace,
	})
}
