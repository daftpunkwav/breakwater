/**
 * @file report_test
 * @description The aggregation over a scripted queryer: the summary
 * folding, the failure-mix classification call, the timeline and
 * dimension mapping, and every error path a healthy database never
 * takes.
 */
package insights

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// fakeQueryer serves one QueryRow answer and one Query result.
type fakeQueryer struct {
	row      scriptedSummaryRow
	rows     *fakeRows
	queryErr error
	rowErr   error
	lastSQL  string
}

type scriptedSummaryRow struct {
	values []any
}

func (f *fakeQueryer) QueryRow(context.Context, string, ...any) pgx.Row {
	return &summaryRow{f: f}
}

type summaryRow struct{ f *fakeQueryer }

func (r *summaryRow) Scan(dest ...any) error {
	if r.f.rowErr != nil {
		return r.f.rowErr
	}
	for i, d := range dest {
		switch ptr := d.(type) {
		case *int64:
			*ptr, _ = r.f.row.values[i].(int64)
		case *float64:
			*ptr, _ = r.f.row.values[i].(float64)
		}
	}
	return nil
}

func (f *fakeQueryer) Query(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
	f.lastSQL = sql
	if f.queryErr != nil {
		return nil, f.queryErr
	}
	// Each query gets a fresh cursor over the same tuples: the report
	// runs several queries against one scripted result set.
	return &fakeRows{tuples: f.rows.tuples, err: f.rows.err}, nil
}

// fakeRows yields scripted tuples of any shape.
type fakeRows struct {
	tuples [][]any
	i      int
	err    error
	closed bool
}

func (r *fakeRows) Next() bool { return r.i < len(r.tuples) }

func (r *fakeRows) Scan(dest ...any) error {
	if r.i >= len(r.tuples) {
		return errors.New("no row")
	}
	tuple := r.tuples[r.i]
	r.i++
	for i, d := range dest {
		if i >= len(tuple) {
			continue // dimension queries scan more columns than the scripted tuples carry
		}
		switch ptr := d.(type) {
		case *string:
			*ptr, _ = tuple[i].(string)
		case *int64:
			*ptr, _ = tuple[i].(int64)
		case *float64:
			*ptr, _ = tuple[i].(float64)
		case *time.Time:
			*ptr, _ = tuple[i].(time.Time)
		}
	}
	return nil
}

func (r *fakeRows) Err() error                                   { return r.err }
func (r *fakeRows) Close()                                       { r.closed = true }
func (r *fakeRows) Values() ([]any, error)                       { return nil, nil }
func (r *fakeRows) RawValues() [][]byte                          { return nil }
func (r *fakeRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *fakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *fakeRows) TypeMap() *pgtype.Map                         { return nil }
func (r *fakeRows) Conn() *pgx.Conn                              { return nil }

func summaryQueryer(requests, failures int64) *fakeQueryer {
	return &fakeQueryer{
		row: scriptedSummaryRow{values: []any{
			requests, failures, 50.0, 95.0, 99.0, int64(500), int64(3),
		}},
		rows: &fakeRows{},
	}
}

func TestReportFoldsSummaryAndDimensions(t *testing.T) {
	t.Parallel()
	q := summaryQueryer(10, 2)
	q.rows.tuples = [][]any{{time.Unix(0, 0).UTC(), int64(10), int64(2), 40.0, 80.0}}

	rep, err := report(context.Background(), q, time.Unix(0, 0), time.Unix(3600, 0).UTC())
	if err != nil {
		t.Fatalf("report: %v", err)
	}
	if rep.Summary.Requests != 10 || rep.Summary.Failures != 2 {
		t.Fatalf("summary = %+v", rep.Summary)
	}
	if rep.Summary.SuccessRate != 0.8 {
		t.Fatalf("success rate = %v, want 0.8", rep.Summary.SuccessRate)
	}
	if rep.Summary.P99MS != 99 || rep.Summary.Tokens != 500 || rep.Summary.CacheHits != 3 {
		t.Fatalf("summary = %+v", rep.Summary)
	}
	if len(rep.Timeline) != 1 || rep.Timeline[0].Requests != 10 {
		t.Fatalf("timeline = %+v", rep.Timeline)
	}
	// All four dimension queries ran.
	for _, dim := range []struct {
		name string
		rows []Dimension
	}{
		{"tenant", rep.ByTenant}, {"key", rep.ByKey}, {"model", rep.ByModel}, {"upstream", rep.ByUpstream},
	} {
		if dim.rows == nil {
			t.Fatalf("%s breakdown missing", dim.name)
		}
	}
}

// TestReportSkipsFailureMixWithoutFailures: a clean window runs no
// failure-classification query.
func TestReportSkipsFailureMixWithoutFailures(t *testing.T) {
	t.Parallel()
	q := summaryQueryer(10, 0)
	rep, err := report(context.Background(), q, time.Unix(0, 0), time.Unix(3600, 0).UTC())
	if err != nil {
		t.Fatalf("report: %v", err)
	}
	if rep.Summary.FailureMix != nil {
		t.Fatalf("failure mix = %v, want none on a clean window", rep.Summary.FailureMix)
	}
}

func TestReportQueryErrorPaths(t *testing.T) {
	t.Parallel()
	boom := errors.New("connection refused")

	// The summary read failing ends the report immediately.
	q := summaryQueryer(10, 2)
	q.rowErr = boom
	if _, err := report(context.Background(), q, time.Unix(0, 0), time.Unix(3600, 0).UTC()); !errors.Is(err, boom) {
		t.Fatalf("summary err = %v, want the cause", err)
	}

	// A failing timeline query fails the report.
	q = summaryQueryer(10, 0)
	q.queryErr = boom
	if _, err := report(context.Background(), q, time.Unix(0, 0), time.Unix(3600, 0).UTC()); !errors.Is(err, boom) {
		t.Fatalf("timeline err = %v, want the cause", err)
	}
}

func TestReportFailureMixClassification(t *testing.T) {
	t.Parallel()
	q := summaryQueryer(10, 3)
	q.rows.tuples = [][]any{
		{"circuit_open", int64(2)},
		{"upstream_5xx", int64(1)},
	}
	rep, err := report(context.Background(), q, time.Unix(0, 0), time.Unix(3600, 0).UTC())
	if err != nil {
		t.Fatalf("report: %v", err)
	}
	if len(rep.Summary.FailureMix) != 2 || rep.Summary.FailureMix[0].Code != "circuit_open" {
		t.Fatalf("mix = %+v", rep.Summary.FailureMix)
	}
}

func TestReportDimensionFolding(t *testing.T) {
	t.Parallel()
	q := summaryQueryer(4, 1)
	q.rows.tuples = [][]any{
		{"tenant-a", int64(3), int64(1), int64(300), 120.5},
		{"-", int64(1), int64(0), int64(0), 90.0},
	}
	rep, err := report(context.Background(), q, time.Unix(0, 0), time.Unix(3600, 0).UTC())
	if err != nil {
		t.Fatalf("report: %v", err)
	}
	if len(rep.ByTenant) != 2 || rep.ByTenant[0].Name != "tenant-a" || rep.ByTenant[0].P95MS != 120.5 {
		t.Fatalf("by tenant = %+v", rep.ByTenant)
	}
	// The dash placeholder marks unattributed rows.
	if rep.ByTenant[1].Name != "-" {
		t.Fatalf("placeholder = %q", rep.ByTenant[1].Name)
	}
}

// countingQueryer fails the Nth query call, then succeeds; the report
// runs one summary row-read, optionally one failure-mix query, one
// timeline query and four dimension queries.
type countingQueryer struct {
	failAt  int
	calls   int
	wrapErr error
}

func (c *countingQueryer) QueryRow(context.Context, string, ...any) pgx.Row {
	c.calls++
	return &summaryRow{f: &fakeQueryer{row: scriptedSummaryRow{values: []any{
		int64(10), int64(2), 50.0, 95.0, 99.0, int64(500), int64(3),
	}}}}
}

func (c *countingQueryer) Query(context.Context, string, ...any) (pgx.Rows, error) {
	c.calls++
	if c.calls == c.failAt {
		return nil, c.wrapErr
	}
	return &fakeRows{tuples: [][]any{{time.Unix(0, 0).UTC(), int64(1), int64(0), 1.0, 2.0}}}, nil
}

// TestReportDimensionErrorPaths: each of the four dimension queries
// failing fails the report with the cause.
func TestReportDimensionErrorPaths(t *testing.T) {
	t.Parallel()
	// Call numbering with a failure mix: 1 summary row, 2 mix query,
	// 3 timeline, 4..7 dimensions.
	for _, failAt := range []int{3, 4, 5, 6, 7} {
		q := &countingQueryer{failAt: failAt, wrapErr: errBoom}
		if _, err := report(context.Background(), q, time.Unix(0, 0), time.Unix(3600, 0).UTC()); !errors.Is(err, errBoom) {
			t.Fatalf("query #%d: err = %v, want the cause", failAt, err)
		}
	}
}

var errBoom = errors.New("boom")
