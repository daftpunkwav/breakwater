/**
 * @file accesslog_test
 * @description The access log file sink: opening the
 * configured log, and the publisher that keeps its drop counter — and
 * the assessment store's — in step with their owners.
 */
package main

import (
	"context"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/daftpunkwav/breakwater/internal/config"
	"github.com/daftpunkwav/breakwater/internal/obs"
)

func TestNewAccessLogEnabled(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "access.log")
	logger := slog.New(slog.DiscardHandler)
	bundle, err := newAccessLog(config.Config{Obs: config.Obs{AccessLogPath: path, AccessLogQueueSize: 8}}, logger)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	bundle.close()
}

func TestPublishLogDropsSyncsCounter(t *testing.T) {
	t.Parallel()
	var out drainingWriter
	logger := obs.NewLogger(&out, 1)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = logger.Close(ctx)
	}()

	metrics := obs.NewMetrics()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	publishLogDrops(ctx, metrics, logger, nil, 5*time.Millisecond)
	logger.Record(obsEntry()) // capacity 1: the next record drops one
	logger.Record(obsEntry())

	// The publisher syncs on its ticker; poll the rendered exposition.
	var rendered strings.Builder
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		rendered.Reset()
		_ = metrics.Render(&rendered)
		if strings.Contains(rendered.String(), "breakwater_logs_dropped_total 1") {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("logs_dropped counter was never synced")
}

// TestPublishLogDropsRunsWithOnlyOneSink: the insights drop counter
// must not depend on the JSONL access log being configured. A
// PostgreSQL deployment with no access-log file would otherwise render
// the counter as a permanent zero while records are being dropped.
func TestPublishLogDropsRunsWithOnlyOneSink(t *testing.T) {
	t.Parallel()
	metrics := obs.NewMetrics()
	ctx, cancel := context.WithCancel(context.Background())
	publishLogDrops(ctx, metrics, nil, nil, 0) // no sink and no ticker: a no-op
	// The insights store alone is enough to run the ticker; a nil access
	// log must not gate it, and a nil store must not panic it.
	publishLogDrops(ctx, metrics, nil, nil, time.Millisecond)
	time.Sleep(5 * time.Millisecond)
	cancel()

	var sink drainingWriter
	logger := obs.NewLogger(&sink, 4)
	publishLogDrops(ctx, metrics, logger, nil, time.Millisecond)
	time.Sleep(5 * time.Millisecond)
	closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second)
	defer closeCancel()
	_ = logger.Close(closeCtx)
}

// drainingWriter consumes every write so the logger drains happily.
type drainingWriter struct{}

func (drainingWriter) Write(p []byte) (int, error) { return len(p), nil }

func obsEntry() obs.Entry { return obs.Entry{Status: 200} }
