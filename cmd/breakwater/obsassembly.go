/**
 * @file obsassembly
 * @description Assembly of the gateway's observation surface: the
 * access log file sink and the admin API bindings.
 *
 * Responsibilities:
 * - Open the access log file when configured and wrap it in the
 *   bounded async logger; drain it before process exit (invariant I8)
 * - Bind the admin endpoints to the live ledger and breaker registry
 * - Nothing else: the log schema and the admin surface live in their
 *   packages; this file only connects them to concrete resources
 */
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/daftpunkwav/breakwater/internal/circuit"
	"github.com/daftpunkwav/breakwater/internal/config"
	"github.com/daftpunkwav/breakwater/internal/obs"
	"github.com/daftpunkwav/breakwater/internal/server"
)

// shutdownLogGrace bounds the drain window beyond the server grace.
const shutdownLogGrace = 5 * time.Second

// accessLog bundles the log sink with its lifecycle.
type accessLog struct {
	sink   obs.Sink
	logger *obs.Logger
	file   *os.File
	once   sync.Once
}

// close drains and shuts the log down; safe to call when disabled and
// safe to call twice — the error-exit path flushes explicitly before
// os.Exit, the normal path through the deferred call.
func (a *accessLog) close() {
	if a == nil || a.logger == nil {
		return
	}
	a.once.Do(func() {
		ctx, cancel := context.WithTimeout(context.Background(), shutdownLogGrace)
		defer cancel()
		if err := a.logger.Close(ctx); err != nil {
			slog.Warn("access log drain incomplete", "error", err)
		}
		if a.file != nil {
			_ = a.file.Close()
		}
	})
}

// newAccessLog opens the JSONL sink when a path is configured; it
// returns a disabled bundle otherwise. An unopenable path is an error,
// not a silently unobserved gateway.
func newAccessLog(cfg config.Config, logger *slog.Logger) (*accessLog, error) {
	if cfg.Obs.AccessLogPath == "" {
		return &accessLog{}, nil
	}
	file, err := os.OpenFile(cfg.Obs.AccessLogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	l := obs.NewLogger(file, cfg.Obs.AccessLogQueueSize)
	logger.Info("access log enabled", "path", cfg.Obs.AccessLogPath, "queue", cfg.Obs.AccessLogQueueSize)
	return &accessLog{sink: l, logger: l, file: file}, nil
}

// buildAdmin binds the admin endpoints to the live backends; options
// forward to server.NewAdmin (the routing switches, the identity
// administration store).
func buildAdmin(cfg config.Config, gov *governance, breaker circuit.Breaker, upstreamIDs []string, opts ...server.AdminOption) http.Handler {
	balances := func(r *http.Request, tenantID string) (int64, error) {
		return gov.ledger.Balance(r.Context(), tenantID)
	}
	setBalance := func(r *http.Request, tenantID string, balance int64) error {
		return gov.ledger.SetBalance(r.Context(), tenantID, balance)
	}
	states := func(r *http.Request) []server.BreakerView {
		views := make([]server.BreakerView, 0, len(upstreamIDs))
		for _, id := range upstreamIDs {
			views = append(views, server.BreakerView{
				Upstream: id,
				State:    breaker.StateOf(r.Context(), id),
			})
		}
		return views
	}
	return server.NewAdmin(cfg.Security.AdminToken, balances, setBalance, states, opts...)
}

// upstreamIDs lists the configured upstream identifiers in order.
func upstreamIDs(cfgs []config.Upstream) []string {
	ids := make([]string, 0, len(cfgs))
	for _, c := range cfgs {
		ids = append(ids, c.ID)
	}
	return ids
}
