# obs/

> Language: **English** | [简体中文](README.zh.md)

The gateway's observation primitives: a bounded asynchronous access log and a hand-written Prometheus-style metrics registry. The package owns the recording mechanics only — which sinks exist and how they are wired is decided in `cmd/breakwater` (the combined fan-out, drop-counter publishing), and the aggregation behind `/admin/insights` lives in `internal/insights`.

## Files

| File | Role |
|---|---|
| `accesslog.go` | Contracts: the append-only `Entry` schema (tenant/key/model/upstream dimensions, status, duration, `CacheHit`, settled `Tokens`, `ErrorCode`, the `Attempts` trail), the async `Sink` port, and `StatusClientClosedRequest` (499) for a request that ended without a response |
| `logger.go` | `Logger`, the JSONL sink: a mutex-guarded ring queue (not a channel — drop-oldest needs eviction) drained by one background goroutine; overflow drops the oldest and counts; `Dropped`/`Written`/`Buffered` expose the accounting; `Flush`/`Close` are shutdown-path only |
| `metrics.go` | `Metrics`: every `breakwater_*` family with typed recorder methods, rendered at scrape time as Prometheus text format 0.0.4 — counters, gauges, two per-upstream histograms (`breakwater_request_duration_seconds`, `breakwater_upstream_ttft_seconds`; `defaultBuckets`, 1 ms–60 s) plus the scalar `breakwater_inflight_requests` gauge |

## Invariants

- `Record` never blocks the request path. Capacity pressure ends in explicit, counted drops — never backpressure, never silent loss. A malformed entry or a failed write lands in the drop counter too, so the accounting stays honest.
- Label vocabulary is bounded: `tenant`, `model`, `upstream`, `status` (plus `from`/`to` for failovers, `result`/`reason` for probes) — no request IDs, no paths. `maxChildren` (4096) caps the label-set leaves per family; past the cap, new sets collapse into one reserved overflow leaf (`*` label values) so the counter keeps moving and the collapse is visible.
- `client_golang` is deliberately absent: the registry is a leaf of atomics, floats ride as bits in `atomic.Uint64` (CAS loop), and label values are escaped strictly (backslash, quote, newline only) because `%q` would emit escapes the exposition parsers reject.
- `Flush` waits for the drain goroutine to fall idle, not merely for the queue to read empty — a failed write lands in the drop counter after the queue is already drained, and the wait must cover it.
- Every `Metrics` recorder method is nil-receiver safe, so call sites record without nil checks.
- The healthy zero is visible: the two drop-counter families (`breakwater_logs_dropped_total`, `breakwater_insights_records_dropped_total`) pre-create their label-less child so the first scrape already shows zero.

Consumed by [internal/pipeline](../../internal/pipeline/) (observation stage) and assembled in [cmd/breakwater](../../cmd/breakwater/); the monitoring records behind `/admin/insights` live in [internal/insights](../../internal/insights/).
