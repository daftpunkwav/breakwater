/**
 * @file metrics
 * @description The gateway's metrics: a hand-written, minimal
 * Prometheus text-exposition registry.
 *
 * Responsibilities:
 * - Define every metric family the evidence documents are built from
 *   (spec §12) with typed, explicit recorder methods
 * - Render the Prometheus text format 0.0.4 at scrape time
 * - Nothing else: no aggregation server-side (quantiles are computed
 *   by the scraper from histogram buckets), no push gateways
 *
 * Discipline note: client_golang is deliberately absent — the registry
 * is a leaf of counters, gauges and histograms, which keeps the
 * dependency surface at zero and the exposition format inspectable.
 * Cardinality discipline: labels are tenant, model, upstream, status —
 * nothing unbounded (no request IDs, no paths).
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

// childOf returns the leaf for a label value tuple.
func (f *family) childOf(values ...string) *child {
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

	inflight atomic.Int64
	logDrops atomic.Int64
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
	reg("breakwater_circuit_state", "Breaker state (0 closed, 1 half-open, 2 open).", "gauge",
		[]string{"upstream"}, nil)

	reg("breakwater_retry_attempts_total", "Retry attempts beyond the first.", "counter",
		[]string{"upstream"}, nil)
	reg("breakwater_retry_budget_exhausted_total", "Requests denied by the retry budget.", "counter", nil, nil)
	reg("breakwater_upstream_failover_total", "Requests that failed over to another upstream.", "counter",
		[]string{"from", "to"}, nil)

	reg("breakwater_sse_stream_aborted_total", "Streams terminated through the error event contract.", "counter",
		[]string{"upstream"}, nil)

	reg("breakwater_logs_dropped_total", "Access log entries dropped for capacity.", "counter", nil, nil)
	// Pre-create the label-less child so the drop counter is exposed —
	// as the healthy zero — from the first scrape, before the periodic
	// sync ever runs.
	m.families["breakwater_logs_dropped_total"].childOf()
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

// SetLogsDropped publishes the access log drop counter; Render sources
// the sample value from here.
func (m *Metrics) SetLogsDropped(n int64) {
	if m == nil {
		return
	}
	m.logDrops.Store(n)
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
					if _, err := fmt.Fprintf(w, "%s_bucket%s %d\n", f.name, labelsWithLE(labels, bound), cumulative); err != nil {
						return err
					}
				}
				if _, err := fmt.Fprintf(w, "%s_bucket%s %d\n", f.name, labelsWithLE(labels, math.Inf(1)), c.count.Load()); err != nil {
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
				if f.name == "breakwater_logs_dropped_total" {
					value = float64(m.logDrops.Load())
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

// renderLabels joins label names and values into the {k="v",...} form.
func renderLabels(names, values []string) string {
	if len(names) == 0 {
		return ""
	}
	pairs := make([]string, len(names))
	for i, name := range names {
		pairs[i] = fmt.Sprintf("%s=%q", name, values[i])
	}
	return "{" + strings.Join(pairs, ",") + "}"
}

// labelsWithLE renders the label set with the histogram le bucket.
func labelsWithLE(rendered string, bound float64) string {
	le := fmt.Sprintf("%g", bound)
	if rendered == "" {
		return "{le=\"" + le + "\"}"
	}
	return strings.Replace(rendered, "}", ",le=\""+le+"\"}", 1)
}
