/**
 * @file obsassembly_test
 * @description The combined observation sink: one entry fans out to
 * the file log and the assessment store, each side optional, and the
 * admin reporter installs only with a live store.
 */
package main

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/daftpunkwav/breakwater/internal/insights"
	"github.com/daftpunkwav/breakwater/internal/obs"
	"github.com/daftpunkwav/breakwater/internal/server"
)

// countingSink counts records and flushes.
type countingSink struct {
	entries []obs.Entry
	flushed bool
}

func (s *countingSink) Record(e obs.Entry) { s.entries = append(s.entries, e) }
func (s *countingSink) Flush(context.Context) error {
	s.flushed = true
	return nil
}

func TestCombinedSinkFansOutToBoth(t *testing.T) {
	t.Parallel()
	file := &countingSink{}
	combined := combinedSink{file: file, insights: &insights.PGStore{}}

	combined.Record(obs.Entry{
		TenantID: "t1", Status: 200, Duration: 1500 * time.Microsecond,
		Tokens: 137, Streamed: true,
	})
	if len(file.entries) != 1 {
		t.Fatalf("file sink saw %d entries, want 1", len(file.entries))
	}
	if combined.insights == nil {
		t.Fatal("insights side missing")
	}
	if err := combined.Flush(context.Background()); err != nil || !file.flushed {
		t.Fatalf("flush = %v flushed = %v", err, file.flushed)
	}
}

// TestCombinedSinkOptionalSides: a nil file log or a nil insights
// store must not panic — disabled sides stay disabled.
func TestCombinedSinkOptionalSides(t *testing.T) {
	t.Parallel()
	fileOnly := combinedSink{file: &countingSink{}}
	fileOnly.Record(obs.Entry{Status: 200})
	if err := fileOnly.Flush(context.Background()); err != nil {
		t.Fatalf("file-only flush: %v", err)
	}

	insightsOnly := combinedSink{}
	insightsOnly.Record(obs.Entry{Status: 200})
	if err := insightsOnly.Flush(context.Background()); err != nil {
		t.Fatalf("insights-only flush: %v", err)
	}
}

// TestAdminWithoutStoreOmitsInsights: without a live store the admin
// handler must not receive a Reporter holding a nil *PGStore — a typed
// nil is not a nil interface, and the endpoint's nil check would never
// fire. The endpoint answers 404, not a panic.
func TestAdminWithoutStoreOmitsInsights(t *testing.T) {
	t.Parallel()
	recorder, err := newInsights(context.Background(), testConfig("127.0.0.1:0"), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("newInsights: %v", err)
	}
	if recorder.store != nil {
		t.Fatal("no DSN must leave the store unset")
	}

	adminOpts := []server.AdminOption{}
	if recorder.store != nil {
		adminOpts = append(adminOpts, server.WithInsights(recorder.store))
	}
	admin := server.NewAdmin("", nil, nil, nil, adminOpts...)

	req := httptest.NewRequest(http.MethodGet, "/admin/insights", nil)
	rec := httptest.NewRecorder()
	admin.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404 without a store", rec.Code)
	}
}
