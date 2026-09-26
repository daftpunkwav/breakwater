/**
 * @file insights
 * @description The monitoring and assessment record: one row per
 * finished request, written asynchronously in batches, aggregated for
 * the admin surface.
 *
 * Responsibilities:
 * - Define the per-request record and the batched writer that
 *   persists it without ever blocking or failing the request path
 * - Aggregate the records into the stability picture operations
 *   needs: success rate, failure mix by cause, latency percentiles,
 *   timelines and per-tenant/key/model/upstream breakdowns
 * - Nothing else: the failure taxonomy itself is stamped by the
 *   pipeline stages (carrier.RejectCode) and the relay
 *   (Result.ErrorCode); this package only stores and reads it
 */
package insights

import "time"

// Record is one finished request's assessment row. It mirrors what the
// observation stage already knows, plus the token usage settlement.
type Record struct {
	Time      time.Time
	TenantID  string
	KeyID     string
	RequestID string
	Model     string
	Upstream  string
	Path      string
	Status    int
	// DurationMS is the end-to-end request duration in milliseconds;
	// milliseconds keep the JSONL and SQL representations aligned.
	DurationMS int64
	Tokens     int64
	CacheHit   bool
	Streamed   bool
	// ErrorCode is the failure classification; empty on success.
	ErrorCode string
}

// Failure is one bucket of the failure mix: a cause and how often it
// happened inside the queried window.
type Failure struct {
	Code  string `json:"code"`
	Count int64  `json:"count"`
}

// SeriesPoint is one time-bucket of the stability timeline.
type SeriesPoint struct {
	Bucket   time.Time `json:"bucket"`
	Requests int64     `json:"requests"`
	Failures int64     `json:"failures"`
	P50MS    float64   `json:"p50_ms"`
	P95MS    float64   `json:"p95_ms"`
}

// Summary is the aggregated stability picture for one window.
type Summary struct {
	// Window the aggregation covers, as requested.
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
	// Requests and Failures count every finished request in the window.
	Requests int64 `json:"requests"`
	Failures int64 `json:"failures"`
	// SuccessRate is (requests - failures) / requests, 0..1; a window
	// without requests reports 0.
	SuccessRate float64 `json:"success_rate"`
	// Latency percentiles over the window, milliseconds.
	P50MS float64 `json:"p50_ms"`
	P95MS float64 `json:"p95_ms"`
	P99MS float64 `json:"p99_ms"`
	// Tokens is the settled token usage the window's requests consumed.
	Tokens int64 `json:"tokens"`
	// CacheHits counts cache-served requests.
	CacheHits int64 `json:"cache_hits"`
	// FailureMix breaks the window's failures down by cause, most
	// frequent first. Upstream passthroughs appear under their status
	// class ("upstream_5xx", "upstream_4xx"); gateway causes under
	// their code.
	FailureMix []Failure `json:"failure_mix"`
}

// Dimension counts one breakdown slice of the window.
type Dimension struct {
	Name     string  `json:"name"`
	Requests int64   `json:"requests"`
	Failures int64   `json:"failures"`
	Tokens   int64   `json:"tokens"`
	P95MS    float64 `json:"p95_ms"`
}

// Breakdown is one per-dimension slice of the window, most traffic
// first.
type Breakdown struct {
	Dimension string      `json:"dimension"`
	Rows      []Dimension `json:"rows"`
}

// Report is the full assessment the admin surface renders: the window
// summary, the stability timeline and the per-dimension breakdowns.
type Report struct {
	Summary    Summary       `json:"summary"`
	Timeline   []SeriesPoint `json:"timeline"`
	ByTenant   []Dimension   `json:"by_tenant"`
	ByKey      []Dimension   `json:"by_key"`
	ByModel    []Dimension   `json:"by_model"`
	ByUpstream []Dimension   `json:"by_upstream"`
}
