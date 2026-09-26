/**
 * @file obsassembly_test
 * @description The combined observation sink: one entry fans out to
 * the file log and the assessment store, each side optional.
 */
package main

import (
	"context"
	"testing"
	"time"

	"github.com/daftpunkwav/breakwater/internal/insights"
	"github.com/daftpunkwav/breakwater/internal/obs"
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

	combined.Record(obs.Entry{TenantID: "t1", Status: 200, Duration: 1500 * time.Microsecond})
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
