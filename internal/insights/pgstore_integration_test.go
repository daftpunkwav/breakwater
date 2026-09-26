/**
 * @file pgstore_integration_test
 * @description The insights store against a real PostgreSQL: records
 * persist in batches, the aggregation folds them into the report, and
 * the failure taxonomy classifies upstream passthroughs by status.
 * Skipped unless BREAKWATER_TEST_POSTGRES_DSN is set (CI provides
 * it).
 */
package insights

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestPGInsightsIntegration(t *testing.T) {
	dsn := os.Getenv("BREAKWATER_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("integration: BREAKWATER_TEST_POSTGRES_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	for _, file := range []string{"../../deploy/schema.sql"} {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		if _, err := conn.Exec(ctx, string(raw)); err != nil {
			t.Fatalf("apply %s: %v", file, err)
		}
	}
	// The report is only honest against this test's own rows.
	if _, err := conn.Exec(ctx, `DELETE FROM request_log WHERE tenant_id = 'insights-int'`); err != nil {
		t.Fatalf("clear previous rows: %v", err)
	}

	store, err := NewPGStore(ctx, dsn)
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	now := time.Now().UTC()
	success := now.Add(-time.Minute)
	store.Record(Record{Time: success, TenantID: "insights-int", KeyID: "k1", RequestID: "r1",
		Model: "m1", Upstream: "u1", Path: "/v1/chat/completions",
		Status: 200, DurationMS: 100, Tokens: 50, CacheHit: true})
	// An upstream 502 passthrough: classified by status, no code.
	store.Record(Record{Time: success, TenantID: "insights-int", KeyID: "k1", RequestID: "r2",
		Model: "m1", Upstream: "u1", Status: 502, DurationMS: 300})
	// A gateway envelope: classified by its code.
	store.Record(Record{Time: success, TenantID: "insights-int", KeyID: "k2", RequestID: "r3",
		Model: "m1", Upstream: "-", Status: 503, DurationMS: 5, ErrorCode: "circuit_open"})
	// A client disconnect: never a failure.
	store.Record(Record{Time: success, TenantID: "insights-int", KeyID: "k1", RequestID: "r4",
		Model: "m1", Upstream: "u1", Status: 499, DurationMS: 20})

	store.insert = store.copyBatch // ensure the production sink
	store.closed = true            // flush synchronously below
	store.mu.Lock()
	batch := store.queue
	store.queue = nil
	store.mu.Unlock()
	store.copyBatch(ctx, batch)
	store.Close()
	store.Close() // idempotent: a second close must not panic

	rep, err := store.Report(ctx, now.Add(-time.Hour), now.Add(time.Hour))
	if err != nil {
		t.Fatalf("report: %v", err)
	}
	if rep.Summary.Requests != 4 {
		t.Fatalf("requests = %d, want 4", rep.Summary.Requests)
	}
	// The 499 disconnect is not a failure; 502 and 503 are.
	if rep.Summary.Failures != 2 {
		t.Fatalf("failures = %d, want 2", rep.Summary.Failures)
	}
	if rep.Summary.SuccessRate != 0.5 {
		t.Fatalf("success rate = %v, want 0.5", rep.Summary.SuccessRate)
	}
	if len(rep.Summary.FailureMix) == 0 {
		t.Fatal("the failure mix is empty")
	}
	mixCode := rep.Summary.FailureMix[0].Code
	if mixCode != "circuit_open" && mixCode != "upstream_5xx" {
		t.Fatalf("top failure = %q, want circuit_open or upstream_5xx", mixCode)
	}
	if rep.Summary.CacheHits != 1 || rep.Summary.Tokens != 50 {
		t.Fatalf("cache hits = %d tokens = %d, want 1/50", rep.Summary.CacheHits, rep.Summary.Tokens)
	}
	if rep.Summary.P50MS == 0 || rep.Summary.P95MS == 0 {
		t.Fatalf("latency percentiles empty: %+v", rep.Summary)
	}
	if len(rep.Timeline) == 0 {
		t.Fatal("timeline empty")
	}
	for _, dim := range []struct {
		name string
		rows []Dimension
	}{{"tenant", rep.ByTenant}, {"key", rep.ByKey}, {"model", rep.ByModel}, {"upstream", rep.ByUpstream}} {
		if len(dim.rows) == 0 {
			t.Fatalf("%s breakdown empty", dim.name)
		}
	}
}

// TestPGInsightsReportEmptyWindow: a window without rows reports an
// empty summary, not a NULL percentile scan failure — the contract the
// summary promises ("a window without requests reports 0").
func TestPGInsightsReportEmptyWindow(t *testing.T) {
	dsn := os.Getenv("BREAKWATER_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("integration: BREAKWATER_TEST_POSTGRES_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	for _, file := range []string{"../../deploy/schema.sql"} {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		if _, err := conn.Exec(ctx, string(raw)); err != nil {
			t.Fatalf("apply %s: %v", file, err)
		}
	}

	store, err := NewPGStore(ctx, dsn)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	defer store.Close()

	rep, err := store.Report(ctx, time.Now().Add(-time.Hour), time.Now().Add(-time.Minute))
	if err != nil {
		t.Fatalf("report on an empty window: %v", err)
	}
	if rep.Summary.Requests != 0 || rep.Summary.SuccessRate != 0 || rep.Summary.P99MS != 0 {
		t.Fatalf("summary = %+v, want the zero report", rep.Summary)
	}
	if rep.Summary.FailureMix != nil {
		t.Fatalf("failure mix = %+v, want none", rep.Summary.FailureMix)
	}
}
