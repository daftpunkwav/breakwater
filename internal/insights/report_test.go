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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
)

// fakeQueryer serves one QueryRow answer and ordered Query results.
type fakeQueryer struct {
	row        scriptedSummaryRow
	resultSets [][][]any
	queryCount int
	queryErr   error
	rowErr     error
	lastSQL    string
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

// resultSets: the tuples each successive Query call returns; the
// report runs the mix (when failures exist), the timeline and four
// dimension queries in that order.
func (f *fakeQueryer) Query(_ context.Context, sql string, _ ...any) (pgx.Rows, error) {
	f.lastSQL = sql
	if f.queryErr != nil {
		return nil, f.queryErr
	}
	idx := f.queryCount
	if idx >= len(f.resultSets) {
		idx = len(f.resultSets) - 1
	}
	f.queryCount++
	return &fakeRows{tuples: f.resultSets[idx]}, nil
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
		v := tuple[i]
		switch ptr := d.(type) {
		case *string:
			s, ok := v.(string)
			if !ok {
				return fmt.Errorf("column %d: got %T, want string", i, v)
			}
			*ptr = s
		case *int64:
			n, ok := v.(int64)
			if !ok {
				return fmt.Errorf("column %d: got %T, want int64", i, v)
			}
			*ptr = n
		case *float64:
			f, ok := v.(float64)
			if !ok {
				return fmt.Errorf("column %d: got %T, want float64", i, v)
			}
			*ptr = f
		case *time.Time:
			ts, ok := v.(time.Time)
			if !ok {
				return fmt.Errorf("column %d: got %T, want time", i, v)
			}
			*ptr = ts
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
		resultSets: [][][]any{{{}}},
	}
}

func TestReportFoldsSummaryAndDimensions(t *testing.T) {
	t.Parallel()
	q := summaryQueryer(10, 2)
	q.resultSets = [][][]any{
		{{"circuit_open", int64(2)}},                               // failure mix
		{{time.Unix(0, 0).UTC(), int64(10), int64(2), 40.0, 80.0}}, // timeline
		{{}}, {{}}, {{}}, {{}}, // dimensions
	}

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
	q.resultSets = [][][]any{
		{{"circuit_open", int64(2)}, {"upstream_5xx", int64(1)}}, // failure mix
		{{}},                   // timeline
		{{}}, {{}}, {{}}, {{}}, // dimensions
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
	q.resultSets = [][][]any{
		{{}}, // failure mix (empty: the summary says 1 failure, mix query runs)
		{{}}, // timeline
		{{"tenant-a", int64(3), int64(1), int64(300), 120.5}, {"-", int64(1), int64(0), int64(0), 90.0}}, // tenant
		{{}}, // key
		{{}}, // model
		{{}}, // upstream
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
	return &fakeRows{tuples: [][]any{{}}}, nil
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

// errorRows surfaces the scripted error on Err after the tuples are
// consumed, pinning the rows.Err propagation of every scan helper.
type errorRows struct {
	fakeRows
	err error
}

func (r *errorRows) Err() error { return r.err }

// failingQueryer returns rows carrying a trailing error.
type failingQueryer struct {
	fakeQueryer
	tuples  [][]any
	rowsErr error
}

func (f *failingQueryer) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return &errorRows{fakeRows: fakeRows{tuples: f.tuples}, err: f.rowsErr}, nil
}

// TestScanHelpersPropagateRowsError: the mix, timeline and dimension
// scans surface a rows.Err as their own error.
func TestScanHelpersPropagateRowsError(t *testing.T) {
	t.Parallel()
	q := &failingQueryer{tuples: [][]any{{"circuit_open", int64(1)}}, rowsErr: errBoom}
	ctx := context.Background()
	window := []time.Time{time.Unix(0, 0).UTC(), time.Unix(3600, 0).UTC()}

	if _, err := failures(ctx, q, window[0], window[1]); !errors.Is(err, errBoom) {
		t.Fatalf("failures err = %v, want the rows error", err)
	}
	if _, err := timeline(ctx, &failingQueryer{tuples: [][]any{{time.Unix(0, 0).UTC(), int64(1), int64(0), 1.0, 2.0}}, rowsErr: errBoom}, window[0], window[1]); !errors.Is(err, errBoom) {
		t.Fatalf("timeline err = %v, want the rows error", err)
	}
	if _, err := dimension(ctx, &failingQueryer{tuples: [][]any{{"tenant-a", int64(1), int64(0), int64(0), 1.0}}, rowsErr: errBoom}, window[0], window[1], "tenant_id"); !errors.Is(err, errBoom) {
		t.Fatalf("dimension err = %v, want the rows error", err)
	}
}

// mismatchRows scans fail: the tuple shapes do not match what the
// scan helpers expect.
type mismatchQueryer struct {
	fakeQueryer
	tuples [][]any
}

func (f *mismatchQueryer) Query(context.Context, string, ...any) (pgx.Rows, error) {
	return &fakeRows{tuples: f.tuples}, nil
}

// TestScanHelpersRejectMismatchedRows: a scan failure inside the loop
// surfaces as the helper's error.
func TestScanHelpersRejectMismatchedRows(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	window := []time.Time{time.Unix(0, 0).UTC(), time.Unix(3600, 0).UTC()}

	badMix := &mismatchQueryer{tuples: [][]any{{int64(1), int64(1)}}} // string slot gets an int
	if _, err := failures(ctx, badMix, window[0], window[1]); err == nil {
		t.Fatal("failures accepted a mismatched row")
	}

	badSeries := &mismatchQueryer{tuples: [][]any{{"not-a-time", "x", "y", "z", "w"}}}
	if _, err := scanSeries(&fakeRows{tuples: badSeries.tuples}); err == nil {
		t.Fatal("scanSeries accepted a mismatched row")
	} else if !strings.Contains(err.Error(), "no row") && !strings.Contains(err.Error(), "time") {
		// Either the fake's shape guard or the type mismatch; both are
		// the scan-failure path.
		t.Logf("scan error = %v", err)
	}
}

// TestReportAndDimensionQueryErrors: the report's timeline-level
// delegation and the mix/dimension Query errors surface as the cause.
func TestReportAndDimensionQueryErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	window := []time.Time{time.Unix(0, 0).UTC(), time.Unix(3600, 0).UTC()}

	// The mix query failing fails the report (the summary says failures exist).
	q := summaryQueryer(10, 3)
	q.queryErr = errBoom
	if _, err := report(ctx, q, window[0], window[1]); !errors.Is(err, errBoom) {
		t.Fatalf("mix query err = %v, want the cause", err)
	}
	// The free helpers surface a Query error directly.
	if _, err := failures(ctx, q, window[0], window[1]); !errors.Is(err, errBoom) {
		t.Fatalf("failures query err = %v", err)
	}
	if _, err := dimension(ctx, q, window[0], window[1], "tenant_id"); !errors.Is(err, errBoom) {
		t.Fatalf("dimension query err = %v", err)
	}
}

// TestDimensionRejectsMismatchedRow: a scan failure inside the
// dimension loop surfaces as the helper's error.
func TestDimensionRejectsMismatchedRow(t *testing.T) {
	t.Parallel()
	q := &mismatchQueryer{tuples: [][]any{{int64(1), int64(1), int64(1), int64(1), int64(1)}}}
	if _, err := dimension(context.Background(), q, time.Unix(0, 0).UTC(), time.Unix(3600, 0).UTC(), "tenant_id"); err == nil {
		t.Fatal("dimension accepted a mismatched row")
	}
}
