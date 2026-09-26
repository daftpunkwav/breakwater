/**
 * @file report
 * @description The aggregation queries behind the assessment surface.
 *
 * Responsibilities:
 * - Fold the request_log rows of one window into the stability
 *   report: summary, failure mix, timeline and per-dimension slices
 * - Nothing else: the window comes from the caller (the admin
 *   handler); the failure taxonomy is whatever the rows carry
 *
 * Failure classification (one rule, applied in SQL): a row is a
 * failure when its status is outside 2xx; its cause is the recorded
 * error code, or — when the gateway passed an upstream error through
 * verbatim — a coarse "upstream_<class>" bucket derived from the
 * status. Client disconnects (status 499) are never a failure.
 */
package insights

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
)

// windowSummaryRow is the summary query's single result row.
type windowSummaryRow struct {
	requests  int64
	failures  int64
	p50       float64
	p95       float64
	p99       float64
	tokens    int64
	cacheHits int64
}

// queryer is the subset of pgxpool the report queries need; the
// concrete pool satisfies it, tests script it.
type queryer interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// Report assembles the full assessment for [from, to). The queries
// scan the window four times; the request_log is append-only and
// indexed on time, which keeps the reads bounded by the window.
func (s *PGStore) Report(ctx context.Context, from, to time.Time) (Report, error) {
	return report(ctx, s.pool, from, to)
}

// report is Report's body over the query port, so every aggregation
// branch — including the error paths a healthy database never takes —
// stays testable against a scripted queryer.
func report(ctx context.Context, q queryer, from, to time.Time) (Report, error) {
	var rep Report
	rep.Summary.From = from
	rep.Summary.To = to

	var row windowSummaryRow
	err := q.QueryRow(ctx, `
		SELECT count(*),
		       count(*) FILTER (WHERE status < 200 OR (status > 299 AND status <> 499)),
		       percentile_cont(0.5)  WITHIN GROUP (ORDER BY duration_ms),
		       percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms),
		       percentile_cont(0.99) WITHIN GROUP (ORDER BY duration_ms),
		       COALESCE(sum(tokens), 0),
		       count(*) FILTER (WHERE cache_hit)
		FROM request_log WHERE time >= $1 AND time < $2`, from, to).
		Scan(&row.requests, &row.failures, &row.p50, &row.p95, &row.p99, &row.tokens, &row.cacheHits)
	if err != nil {
		return Report{}, err
	}
	rep.Summary.Requests = row.requests
	rep.Summary.Failures = row.failures
	rep.Summary.P50MS, rep.Summary.P95MS, rep.Summary.P99MS = row.p50, row.p95, row.p99
	rep.Summary.Tokens = row.tokens
	rep.Summary.CacheHits = row.cacheHits
	if row.requests > 0 {
		rep.Summary.SuccessRate = float64(row.requests-row.failures) / float64(row.requests)
	}

	if rep.Summary.Failures > 0 {
		mix, err := failures(ctx, q, from, to)
		if err != nil {
			return Report{}, err
		}
		rep.Summary.FailureMix = mix
	}

	if rep.Timeline, err = timeline(ctx, q, from, to); err != nil {
		return Report{}, err
	}
	if rep.ByTenant, err = dimension(ctx, q, from, to, "tenant_id"); err != nil {
		return Report{}, err
	}
	if rep.ByKey, err = dimension(ctx, q, from, to, "key_id"); err != nil {
		return Report{}, err
	}
	if rep.ByModel, err = dimension(ctx, q, from, to, "model"); err != nil {
		return Report{}, err
	}
	if rep.ByUpstream, err = dimension(ctx, q, from, to, "upstream"); err != nil {
		return Report{}, err
	}
	return rep, nil
}

// failures breaks the window's failures down by cause, most frequent
// first. Rows without a recorded code are upstream error
// passthroughs: their status is the classification.
func failures(ctx context.Context, q queryer, from, to time.Time) ([]Failure, error) {
	rows, err := q.Query(ctx, `
		SELECT COALESCE(NULLIF(error_code, ''),
		                CASE WHEN status >= 500 THEN 'upstream_5xx' ELSE 'upstream_4xx' END) AS cause,
		       count(*)
		FROM request_log
		WHERE time >= $1 AND time < $2
		  AND (status < 200 OR (status > 299 AND status <> 499))
		GROUP BY cause ORDER BY count(*) DESC`, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Failure
	for rows.Next() {
		var f Failure
		if err := rows.Scan(&f.Code, &f.Count); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}

// timeline buckets the window into fixed 5-minute slots — coarse
// enough to stay a bounded series, fine enough to see an incident.
// date_bin keeps the query dependency-free (no timescaledb).
func timeline(ctx context.Context, q queryer, from, to time.Time) ([]SeriesPoint, error) {
	rows, err := q.Query(ctx, `
		SELECT date_bin('5 minutes', time, $3) AS bucket, count(*),
		       count(*) FILTER (WHERE status < 200 OR (status > 299 AND status <> 499)),
		       percentile_cont(0.5)  WITHIN GROUP (ORDER BY duration_ms),
		       percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms)
		FROM request_log
		WHERE time >= $1 AND time < $2
		GROUP BY bucket ORDER BY bucket`, from, to, from)
	if err != nil {
		return nil, err
	}
	return scanSeries(rows)
}

// dimension is the shared per-slice breakdown query; column is one of
// the fixed dimension names this file calls it with, never user input.
func dimension(ctx context.Context, q queryer, from, to time.Time, column string) ([]Dimension, error) {
	rows, err := q.Query(ctx, `
		SELECT COALESCE(NULLIF(`+column+`, ''), '-') AS name,
		       count(*),
		       count(*) FILTER (WHERE status < 200 OR (status > 299 AND status <> 499)),
		       COALESCE(sum(tokens), 0),
		       percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms)
		FROM request_log WHERE time >= $1 AND time < $2
		GROUP BY name ORDER BY count(*) DESC LIMIT 20`, from, to)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Dimension
	for rows.Next() {
		var d Dimension
		if err := rows.Scan(&d.Name, &d.Requests, &d.Failures, &d.Tokens, &d.P95MS); err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func scanSeries(rows pgx.Rows) ([]SeriesPoint, error) {
	defer rows.Close()
	var out []SeriesPoint
	for rows.Next() {
		var p SeriesPoint
		if err := rows.Scan(&p.Bucket, &p.Requests, &p.Failures, &p.P50MS, &p.P95MS); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
