/**
 * @file metrics
 * @description The gateway's metrics: a hand-written, minimal
 * Prometheus text-exposition registry.
 *
 * Responsibilities:
 * - Define every metric family the Prometheus exposition publishes,
 *   each with typed, explicit recorder methods
 * - Render the Prometheus text format 0.0.4 at scrape time
 * - Nothing else: no aggregation server-side (quantiles are computed
 *   by the scraper from histogram buckets), no push gateways
 *
 * Discipline note: client_golang is deliberately absent — the registry
 * is a leaf of counters, gauges and histograms, which keeps the
 * dependency surface at zero and the exposition format inspectable.
 * Cardinality discipline: labels are tenant, model, upstream, status —
 * no request IDs, no paths. The model label comes from client input
 * (it is the requested model name, known before routing), so it is not
 * intrinsically bounded; the per-family child cap below bounds what a
 * hostile or buggy client can grow, collapsing every label set past the
 * cap into one reserved leaf rather than dropping it, so the counter
 * keeps moving and the collapse is visible in the exposition.
 */
package obs

import (
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
)

// defaultBuckets are the latency buckets in seconds, covering the
// sub-millisecond forwarding path out to slow upstreams.
var defaultBuckets = []float64{.001, .0025, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60}

// child is one label-set leaf of a family. The float value is stored
// as bits in a Uint64: the project supports toolchains whose
// sync/atomic lacks the dedicated float type (see addFloat).
type child struct {
	values  []string
	value   atomic.Uint64
	count   atomic.Uint64
	buckets []atomic.Uint64
}

// addFloat atomically adds delta to a float64 held as bits.
func addFloat(addr *atomic.Uint64, delta float64) {
	for {
		old := addr.Load()
		next := math.Float64bits(math.Float64frombits(old) + delta)
		if addr.CompareAndSwap(old, next) {
			return
		}
	}
}

// loadFloat reads a float64 held as bits.
func loadFloat(addr *atomic.Uint64) float64 {
	return math.Float64frombits(addr.Load())
}

// family is one metric family (one HELP/TYPE block).
type family struct {
	name     string
	help     string
	typ      string
	labels   []string
	buckets  []float64
	children map[string]*child

	mu sync.RWMutex
}

// maxChildren caps the label-set leaves of one family. The configured
// label vocabulary keeps every legitimate deployment far below it; the
// cap only bounds what a hostile or buggy client (an unbounded model
// name) can grow. Past the cap new label sets collapse into one
// reserved leaf instead of being dropped, so the exposition keeps
// showing that the traffic happened and says which labels it lost —
// where a counter that silently stops moving for the life of the
// process would be the worse failure. Counters and histograms in the
// overflow leaf are still correct in aggregate; a gauge collapsed this
// way reports the last writer, not a total.
const maxChildren = 4096

// labelCapOverflow is the placeholder value every label takes in the
// reserved overflow leaf.
const labelCapOverflow = "*"

// overflowKey is the reserved map key of that leaf. It cannot collide
// with a real label tuple, which joins its values with "\x00".
const overflowKey = "\x00overflow"

// overflowValues is the placeholder tuple for a family with n labels.
func overflowValues(n int) []string {
	return make([]string, n)
}

// childOf returns the leaf for a label value tuple. It never returns
// nil: past the family's cap every further tuple shares the reserved
// overflow leaf, so the family keeps accumulating and the loss of
// per-label detail is bounded, visible and recoverable (a restart
// clears it) instead of silently permanent.
func (f *family) childOf(values ...string) *child {
	// A client-controlled label (the model name) carrying NUL could
	// forge another tuple's key — or the reserved overflow leaf's — by
	// smuggling the join separator, and the raw byte would poison the
	// rendered exposition. Scrub it from the values first; the key and
	// the stored labels read the cleaned form.
	for _, v := range values {
		if strings.IndexByte(v, 0) >= 0 {
			clean := make([]string, len(values))
			for i, s := range values {
				clean[i] = strings.ReplaceAll(s, "\x00", "")
			}
			values = clean
			break
		}
	}
	key := strings.Join(values, "\x00")
	f.mu.RLock()
	c, ok := f.children[key]
	f.mu.RUnlock()
	if ok {
		return c
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if existing, ok := f.children[key]; ok {
		return existing
	}
	if len(f.children) >= maxChildren {
		ov, ok := f.children[overflowKey]
		if !ok {
			ov = &child{values: overflowValues(len(f.labels))}
			for i := range ov.values {
				ov.values[i] = labelCapOverflow
			}
			if f.typ == "histogram" {
				ov.buckets = make([]atomic.Uint64, len(f.buckets))
			}
			f.children[overflowKey] = ov
		}
		return ov
	}
	leaf := &child{values: values}
	if f.typ == "histogram" {
		leaf.buckets = make([]atomic.Uint64, len(f.buckets))
	}
	f.children[key] = leaf
	return leaf
}

// Metrics records every governance event worth graphing. It is safe
// for concurrent use.
type Metrics struct {
	families map[string]*family

	inflight     atomic.Int64
	logDrops     atomic.Int64
	insightDrops atomic.Int64
}

// NewMetrics builds the registry with every family declared.
func NewMetrics() *Metrics {
	m := &Metrics{families: map[string]*family{}}
	reg := func(name, help, typ string, labels []string, buckets []float64) *family {
		f := &family{
			name: name, help: help, typ: typ,
			labels: labels, buckets: buckets,
			children: map[string]*child{},
		}
		m.families[name] = f
		return f
	}

	reg("breakwater_requests_total", "Client requests by outcome.", "counter",
		[]string{"tenant", "model", "upstream", "status"}, nil)
	reg("breakwater_request_duration_seconds", "End-to-end request duration.", "histogram",
		[]string{"upstream"}, defaultBuckets)

	reg("breakwater_rate_limited_total", "Requests rejected by the rate limiter.", "counter",
		[]string{"tenant"}, nil)
	reg("breakwater_concurrency_limited_total", "Requests rejected by the per-tenant concurrency ceiling.", "counter",
		[]string{"tenant"}, nil)

	reg("breakwater_quota_reservation_tokens_total", "Tokens reserved by the quota ledger.", "counter",
		[]string{"tenant"}, nil)
	reg("breakwater_quota_refunded_tokens_total", "Tokens refunded at settlement.", "counter",
		[]string{"tenant"}, nil)
	reg("breakwater_quota_reservation_expired_total", "Leases reclaimed by the sweeper.", "counter", nil, nil)
	reg("breakwater_quota_reconciliation_error", "Ledger identity drifts detected by the reconcile protocol; always zero when the ledger is healthy.", "counter", nil, nil)

	reg("breakwater_cache_hit_total", "Responses served from the cache.", "counter", nil, nil)
	reg("breakwater_cache_miss_total", "Cache lookups that missed.", "counter", nil, nil)
	reg("breakwater_cache_upstream_fetch_total", "Upstream fetches on the cache path.", "counter",
		[]string{"upstream"}, nil)
	reg("breakwater_cache_shared_fetch_total", "Requests served by a shared singleflight fetch.", "counter", nil, nil)

	reg("breakwater_circuit_open_total", "Breaker open transitions.", "counter",
		[]string{"upstream"}, nil)
	reg("breakwater_circuit_half_open_total", "Breaker half-open transitions.", "counter",
		[]string{"upstream"}, nil)
	reg("breakwater_circuit_denied_total", "Calls denied by the ratio-strategy breaker.", "counter",
		[]string{"upstream"}, nil)
	reg("breakwater_circuit_state", "Breaker state (0 closed, 1 half-open, 2 open).", "gauge",
		[]string{"upstream"}, nil)

	reg("breakwater_retry_attempts_total", "Retry attempts beyond the first.", "counter",
		[]string{"upstream"}, nil)
	reg("breakwater_retry_budget_exhausted_total", "Requests denied by the retry budget.", "counter", nil, nil)
	reg("breakwater_upstream_failover_total", "Requests that failed over to another upstream.", "counter",
		[]string{"from", "to"}, nil)

	reg("breakwater_sse_stream_aborted_total", "Streams terminated through the error event contract.", "counter",
		[]string{"upstream"}, nil)

	reg("breakwater_upstream_probe_total", "Active health probes by outcome.", "counter",
		[]string{"upstream", "result"}, nil)
	reg("breakwater_upstream_auto_disabled_total", "Upstreams taken out of rotation by a fatal upstream condition.", "counter",
		[]string{"upstream", "reason"}, nil)
	reg("breakwater_credential_retired_total", "Credentials retired by a fatal credential condition while others kept the upstream serving.", "counter",
		[]string{"upstream", "reason"}, nil)
	reg("breakwater_upstream_ttft_seconds", "Time to first byte of streaming replies, per upstream.", "histogram",
		[]string{"upstream"}, defaultBuckets)

	reg("breakwater_logs_dropped_total", "Access log entries dropped for capacity.", "counter", nil, nil)
	// Pre-create the label-less child so the drop counter is exposed —
	// as the healthy zero — from the first scrape, before the periodic
	// sync ever runs.
	m.families["breakwater_logs_dropped_total"].childOf()
	reg("breakwater_insights_records_dropped_total", "Insight records dropped for capacity or after shutdown.", "counter", nil, nil)
	m.families["breakwater_insights_records_dropped_total"].childOf()
	return m
}

func (m *Metrics) inc(name string, amount float64, values ...string) {
	c := m.families[name].childOf(values...)
	addFloat(&c.value, amount)
	c.count.Add(1)
}

// Request records one finished client request.
func (m *Metrics) Request(tenant, model, upstream string, status int) {
	if m == nil {
		return
	}
	m.inc("breakwater_requests_total", 1, tenant, model, upstream, fmt.Sprint(status))
}

// ObserveDuration records the request duration in seconds.
func (m *Metrics) ObserveDuration(upstream string, seconds float64) {
	if m == nil {
		return
	}
	f := m.families["breakwater_request_duration_seconds"]
	c := f.childOf(upstream)
	addFloat(&c.value, seconds)
	c.count.Add(1)
	for i, bound := range f.buckets {
		if seconds <= bound {
			c.buckets[i].Add(1)
		}
	}
}

// InflightAdd adjusts the in-flight gauge.
func (m *Metrics) InflightAdd(delta int64) {
	if m == nil {
		return
	}
	m.inflight.Add(delta)
}

// Inflight reports the requests currently in flight — the live traffic
// figure share-mode budgets scale their caps against.
func (m *Metrics) Inflight() int64 {
	if m == nil {
		return 0
	}
	return m.inflight.Load()
}

// RateLimited records a rate limit rejection.
func (m *Metrics) RateLimited(tenant string) {
	if m == nil {
		return
	}
	m.inc("breakwater_rate_limited_total", 1, tenant)
}

// ConcurrencyLimited records a concurrency-ceiling rejection.
func (m *Metrics) ConcurrencyLimited(tenant string) {
	if m == nil {
		return
	}
	m.inc("breakwater_concurrency_limited_total", 1, tenant)
}

// QuotaReserved records the token estimate reserved for a request.
func (m *Metrics) QuotaReserved(tenant string, tokens int64) {
	if m == nil {
		return
	}
	m.inc("breakwater_quota_reservation_tokens_total", float64(tokens), tenant)
}

// QuotaRefunded records tokens refunded at settlement.
func (m *Metrics) QuotaRefunded(tenant string, tokens int64) {
	if m == nil {
		return
	}
	m.inc("breakwater_quota_refunded_tokens_total", float64(tokens), tenant)
}

// QuotaReconciliationError records one detected ledger drift.
func (m *Metrics) QuotaReconciliationError() {
	if m == nil {
		return
	}
	m.inc("breakwater_quota_reconciliation_error", 1)
}

// QuotaExpired records a lease reclaimed by the sweeper.
func (m *Metrics) QuotaExpired() {
	if m == nil {
		return
	}
	m.inc("breakwater_quota_reservation_expired_total", 1)
}

// CacheHit records a cache-served response (hit replay or shared fetch).
func (m *Metrics) CacheHit() {
	if m == nil {
		return
	}
	m.inc("breakwater_cache_hit_total", 1)
}

// CacheMiss records a cache miss.
func (m *Metrics) CacheMiss() {
	if m == nil {
		return
	}
	m.inc("breakwater_cache_miss_total", 1)
}

// CacheFetch records an upstream fetch on the cache path.
func (m *Metrics) CacheFetch(upstream string) {
	if m == nil {
		return
	}
	m.inc("breakwater_cache_upstream_fetch_total", 1, upstream)
}

// CacheShared records a request served by someone else's fetch.
func (m *Metrics) CacheShared() {
	if m == nil {
		return
	}
	m.inc("breakwater_cache_shared_fetch_total", 1)
}

// CircuitOpened records an open transition.
func (m *Metrics) CircuitOpened(upstream string) {
	if m == nil {
		return
	}
	m.inc("breakwater_circuit_open_total", 1, upstream)
}

// CircuitHalfOpen records a half-open transition.
func (m *Metrics) CircuitHalfOpen(upstream string) {
	if m == nil {
		return
	}
	m.inc("breakwater_circuit_half_open_total", 1, upstream)
}

// CircuitDenied records a call denied by the ratio-strategy breaker.
func (m *Metrics) CircuitDenied(upstream string) {
	if m == nil {
		return
	}
	m.inc("breakwater_circuit_denied_total", 1, upstream)
}

// CircuitState publishes the current breaker state as a gauge.
func (m *Metrics) CircuitState(upstream string, stateValue float64) {
	if m == nil {
		return
	}
	m.families["breakwater_circuit_state"].childOf(upstream).value.Store(math.Float64bits(stateValue))
}

// RetryScheduled records a retry attempt beyond the first.
func (m *Metrics) RetryScheduled(upstream string) {
	if m == nil {
		return
	}
	m.inc("breakwater_retry_attempts_total", 1, upstream)
}

// RetryBudgetExhausted records a request denied by the retry budget.
func (m *Metrics) RetryBudgetExhausted() {
	if m == nil {
		return
	}
	m.inc("breakwater_retry_budget_exhausted_total", 1)
}

// Failover records a failover between two upstreams.
func (m *Metrics) Failover(from, to string) {
	if m == nil {
		return
	}
	m.inc("breakwater_upstream_failover_total", 1, from, to)
}

// StreamAborted records a stream terminated through the error contract.
func (m *Metrics) StreamAborted(upstream string) {
	if m == nil {
		return
	}
	m.inc("breakwater_sse_stream_aborted_total", 1, upstream)
}

// UpstreamProbe records one active health probe outcome.
func (m *Metrics) UpstreamProbe(upstream string, ok bool) {
	if m == nil {
		return
	}
	result := "fail"
	if ok {
		result = "ok"
	}
	m.inc("breakwater_upstream_probe_total", 1, upstream, result)
}

// UpstreamAutoDisabled records an upstream taken out of rotation by a
// fatal upstream condition.
func (m *Metrics) UpstreamAutoDisabled(upstream, reason string) {
	if m == nil {
		return
	}
	m.inc("breakwater_upstream_auto_disabled_total", 1, upstream, reason)
}

// CredentialRetired records a credential retired by a fatal credential
// condition while other credentials kept the upstream serving.
func (m *Metrics) CredentialRetired(upstream, reason string) {
	if m == nil {
		return
	}
	m.inc("breakwater_credential_retired_total", 1, upstream, reason)
}

// ObserveTTFT records the time to first byte of one streaming reply.
// Buffered replies are covered by the end-to-end duration histogram.
func (m *Metrics) ObserveTTFT(upstream string, seconds float64) {
	if m == nil {
		return
	}
	f := m.families["breakwater_upstream_ttft_seconds"]
	c := f.childOf(upstream)
	addFloat(&c.value, seconds)
	c.count.Add(1)
	for i, bound := range f.buckets {
		if seconds <= bound {
			c.buckets[i].Add(1)
		}
	}
}

// SetLogsDropped publishes the access log drop counter; Render sources
// the sample value from here.
func (m *Metrics) SetLogsDropped(n int64) {
	if m == nil {
		return
	}
	m.logDrops.Store(n)
}

// SetInsightsDropped publishes the insights store drop counter; Render
// sources the sample value from here, like SetLogsDropped.
func (m *Metrics) SetInsightsDropped(n int64) {
	if m == nil {
		return
	}
	m.insightDrops.Store(n)
}

// Render writes every family in the Prometheus text format 0.0.4,
// returning the first write failure.
func (m *Metrics) Render(w io.Writer) error {
	names := make([]string, 0, len(m.families))
	for name := range m.families {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		f := m.families[name]
		if _, err := fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n", f.name, f.help, f.name, f.typ); err != nil {
			return err
		}
		f.mu.RLock()
		children := make([]*child, 0, len(f.children))
		for _, c := range f.children {
			children = append(children, c)
		}
		f.mu.RUnlock()

		switch f.typ {
		case "histogram":
			for _, c := range children {
				labels := renderLabels(f.labels, c.values)
				var cumulative uint64
				for i, bound := range f.buckets {
					cumulative = c.buckets[i].Load()
					if _, err := fmt.Fprintf(w, "%s_bucket%s %d\n", f.name, renderLabels(f.labels, c.values, lePair(bound)), cumulative); err != nil {
						return err
					}
				}
				if _, err := fmt.Fprintf(w, "%s_bucket%s %d\n", f.name, renderLabels(f.labels, c.values, lePair(math.Inf(1))), c.count.Load()); err != nil {
					return err
				}
				if _, err := fmt.Fprintf(w, "%s_sum%s %g\n", f.name, labels, loadFloat(&c.value)); err != nil {
					return err
				}
				if _, err := fmt.Fprintf(w, "%s_count%s %d\n", f.name, labels, c.count.Load()); err != nil {
					return err
				}
			}
		default:
			for _, c := range children {
				labels := renderLabels(f.labels, c.values)
				value := loadFloat(&c.value)
				switch f.name {
				case "breakwater_logs_dropped_total":
					value = float64(m.logDrops.Load())
				case "breakwater_insights_records_dropped_total":
					value = float64(m.insightDrops.Load())
				}
				if _, err := fmt.Fprintf(w, "%s%s %g\n", f.name, labels, value); err != nil {
					return err
				}
			}
		}
	}
	// Scalars live outside families.
	_, err := fmt.Fprintf(w, "# HELP breakwater_inflight_requests Requests currently in flight.\n# TYPE breakwater_inflight_requests gauge\nbreakwater_inflight_requests %d\n", m.inflight.Load())
	return err
}

// labelEscaper escapes label values per the Prometheus text format:
// backslash, double quote and newline only, every other byte literal.
// Go's %q would also emit escapes the exposition parsers reject (\t,
// \x.., \u....) — one such value would poison the whole scraped
// payload, so values are escaped strictly here instead.
var labelEscaper = strings.NewReplacer(
	`\`, `\\`,
	`"`, `\"`,
	"\n", `\n`,
)

// renderLabels joins label names and values into the {k="v",...} form.
// extraPairs, when given, render as trailing pairs (the histogram le
// bucket); each must already be a fully rendered "name=\"value\"" pair.
func renderLabels(names, values []string, extraPairs ...string) string {
	if len(names) == 0 && len(extraPairs) == 0 {
		return ""
	}
	pairs := make([]string, 0, len(names)+len(extraPairs))
	for i, name := range names {
		pairs = append(pairs, name+`="`+labelEscaper.Replace(values[i])+`"`)
	}
	pairs = append(pairs, extraPairs...)
	return "{" + strings.Join(pairs, ",") + "}"
}

// lePair renders the histogram bucket boundary as a label pair.
func lePair(bound float64) string {
	return `le="` + fmt.Sprintf("%g", bound) + `"`
}
