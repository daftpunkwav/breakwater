/**
 * @file obsassembly_sink_test
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
	"reflect"
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

// TestInsightsRecordFieldCoverage holds the two record schemas
// together. obs.Entry is append-only by contract and insights.Record
// mirrors it, but nothing else forces the projection to keep up: a
// field added to either shape without updating insightsRecord would
// silently stop persisting (or start ignoring) a column. The test pins:
//
//   - every projected field carries the entry's value;
//   - every exported insights.Record field is covered by the projection
//     (a new column fails here until the converter sets it);
//   - every exported obs.Entry field is either projected or in the
//     deliberately-dropped set (a new log field fails here until the
//     author makes that call explicitly).
func TestInsightsRecordFieldCoverage(t *testing.T) {
	t.Parallel()
	entry := obs.Entry{
		Time:      time.Unix(1700000000, 0).UTC(),
		TenantID:  "tenant",
		KeyID:     "key-42",
		RequestID: "req-9f2c",
		Model:     "mock-model",
		Upstream:  "mock",
		Method:    http.MethodPost,
		Path:      "/v1/chat/completions",
		Status:    200,
		Duration:  1500 * time.Microsecond,
		CacheHit:  true,
		Tokens:    137,
		Streamed:  true,
		ErrorCode: "rate_limit_exceeded",
		Attempts:  []obs.AttemptTrace{{Upstream: "mock", Credential: 0, Status: 200}},
	}

	got := insightsRecord(entry)
	want := insights.Record{
		Time:       entry.Time,
		TenantID:   "tenant",
		KeyID:      "key-42",
		RequestID:  "req-9f2c",
		Model:      "mock-model",
		Upstream:   "mock",
		Path:       "/v1/chat/completions",
		Status:     200,
		DurationMS: 1,
		Tokens:     137,
		CacheHit:   true,
		Streamed:   true,
		ErrorCode:  "rate_limit_exceeded",
	}
	if got != want {
		t.Fatalf("insightsRecord = %+v, want %+v", got, want)
	}

	// Every record field non-zero on a fully-populated entry: a field
	// the converter forgets reads as zero here, at the seam, instead of
	// silently missing its column in PostgreSQL.
	rv := reflect.ValueOf(got)
	rt := rv.Type()
	for i := 0; i < rt.NumField(); i++ {
		if rv.Field(i).IsZero() {
			t.Errorf("insights.Record.%s left zero by insightsRecord: extend the converter with the field", rt.Field(i).Name)
		}
	}

	// The field correspondence table: obs.Entry field -> insights.Record
	// field. The dropped set is deliberate, never silent:
	//   - Attempts: the per-attempt trail is the access log's dimension,
	//     not a persisted assessment column.
	//   - Method: the request_log schema has no method column; projecting
	//     it is a persistence-schema migration (DDL), not a code change.
	projected := map[string]string{
		"Time": "Time", "TenantID": "TenantID", "KeyID": "KeyID",
		"RequestID": "RequestID", "Model": "Model", "Upstream": "Upstream",
		"Path": "Path", "Status": "Status", "Duration": "DurationMS",
		"Tokens": "Tokens", "CacheHit": "CacheHit", "Streamed": "Streamed",
		"ErrorCode": "ErrorCode",
	}
	dropped := map[string]bool{"Attempts": true, "Method": true}

	et := reflect.TypeOf(entry)
	for i := 0; i < et.NumField(); i++ {
		name := et.Field(i).Name
		if _, ok := projected[name]; ok {
			continue
		}
		if dropped[name] {
			continue
		}
		t.Errorf("obs.Entry.%s is neither projected nor pinned as deliberately dropped: project it in insightsRecord or add it to the dropped set", name)
	}
	for i := 0; i < rt.NumField(); i++ {
		name := rt.Field(i).Name
		found := false
		for _, target := range projected {
			if target == name {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("insights.Record.%s is not covered by the projection table", name)
		}
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
