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

// openIntegrationPG dials the DSN-gated integration database, skipping
// the test when BREAKWATER_TEST_POSTGRES_DSN is unset, applies the
// deploy schema, and returns the bounded context, the connection and
// the DSN.
func openIntegrationPG(t *testing.T) (context.Context, *pgx.Conn, string) {
	t.Helper()
	dsn := os.Getenv("BREAKWATER_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("integration: BREAKWATER_TEST_POSTGRES_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close(ctx) })
	for _, file := range []string{"../../deploy/schema.sql"} {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		if _, err := conn.Exec(ctx, string(raw)); err != nil {
			t.Fatalf("apply %s: %v", file, err)
		}
	}
	return ctx, conn, dsn
}

// requireIntegrationSummary asserts the aggregate the four scripted
// records must fold into: two upstream failures out of four requests,
// one cache hit, real latency percentiles and a classified failure mix.
func requireIntegrationSummary(t *testing.T, s Summary) {
	t.Helper()
	if s.Requests != 4 {
		t.Fatalf("requests = %d, want 4", s.Requests)
	}
	// The 499 disconnect is not a failure; 502 and 503 are.
	if s.Failures != 2 {
		t.Fatalf("failures = %d, want 2", s.Failures)
	}
	if s.SuccessRate != 0.5 {
		t.Fatalf("success rate = %v, want 0.5", s.SuccessRate)
	}
	if len(s.FailureMix) == 0 {
		t.Fatal("the failure mix is empty")
	}
	mixCode := s.FailureMix[0].Code
	if mixCode != "circuit_open" && mixCode != "upstream_5xx" {
		t.Fatalf("top failure = %q, want circuit_open or upstream_5xx", mixCode)
	}
	if s.CacheHits != 1 || s.Tokens != 50 {
		t.Fatalf("cache hits = %d tokens = %d, want 1/50", s.CacheHits, s.Tokens)
	}
	if s.P50MS == 0 || s.P95MS == 0 {
		t.Fatalf("latency percentiles empty: %+v", s)
	}
}

func TestPGInsightsIntegration(t *testing.T) {
	ctx, conn, dsn := openIntegrationPG(t)
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
	requireIntegrationSummary(t, rep.Summary)
	if len(rep.Timeline) == 0 {
		t.Fatal("timeline empty")
	}
	// Each breakdown groups by its own column, and the four dimensions
	// carry distinct values here: a CASE branch swapped onto the wrong
	// column names the wrong busiest slice. That is the one property the
	// scripted unit test cannot reach, because it never runs the
	// statement — it only pins the text.
	for _, dim := range []struct {
		name string
		rows []Dimension
		want string
	}{
		{"tenant", rep.ByTenant, "insights-int"},
		{"key", rep.ByKey, "k1"},
		{"model", rep.ByModel, "m1"},
		{"upstream", rep.ByUpstream, "u1"},
	} {
		if len(dim.rows) == 0 {
			t.Fatalf("%s breakdown empty", dim.name)
		}
		if dim.rows[0].Name != dim.want {
			t.Fatalf("%s busiest slice = %q, want %q (%+v)", dim.name, dim.rows[0].Name, dim.want, dim.rows)
		}
	}
}

// TestPGInsightsReportEmptyWindow: a window without rows reports an
// empty summary, not a NULL percentile scan failure — the contract the
// summary promises ("a window without requests reports 0").
func TestPGInsightsReportEmptyWindow(t *testing.T) {
	ctx, conn, dsn := openIntegrationPG(t)

	store, err := NewPGStore(ctx, dsn)
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	defer store.Close()

	// The window must hold nothing: earlier tests wrote rows into the
	// shared database inside the same recent span, so clear the span
	// before asserting the empty-window contract.
	from := time.Now().Add(-time.Hour)
	if _, err := conn.Exec(ctx, `DELETE FROM request_log WHERE time >= $1`, from); err != nil {
		t.Fatalf("clear the window: %v", err)
	}

	rep, err := store.Report(ctx, from, time.Now().Add(-time.Minute))
	if err != nil {
		t.Fatalf("report on an empty window: %v", err)
	}
	if rep.Summary.Requests != 0 || rep.Summary.SuccessRate != 0 || rep.Summary.P99MS != 0 {
		t.Fatalf("summary = %+v, want the zero report", rep.Summary)
	}
	// An empty window renders empty lists, never null.
	if len(rep.Summary.FailureMix) != 0 {
		t.Fatalf("failure mix = %+v, want none", rep.Summary.FailureMix)
	}
}
