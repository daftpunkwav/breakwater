# Chaos report

> Language: **English** | [简体中文](CHAOS-REPORT.zh.md)

> Status: the fault-injection procedures below are final and scripted;
> the timelines are filled in by executing them. Nothing here is
> simulated on paper — an empty section means "not yet executed on
> record".

## Method

Faults are injected against a running stack, never by mocking inside
the gateway. Each experiment records: what was broken, when, what the
clients saw (status codes and latency), and what the recovery looked
like on `/metrics`.

## Experiments

### 1. Upstream total failure → breaker timeline

- Inject: `mockllm -error-rate 1.0`
- Observe: consecutive failures open the breaker; client-perceived
  failure latency collapses from timeout-scale to fast 503; after the
  cooldown exactly one probe crosses; on recovery the state walks
  half-open → closed.
- Evidence: `breakwater_circuit_open_total`,
  `breakwater_circuit_half_open_total`, `breakwater_circuit_state`, plus
  the k6 `failure_latency` trend (`loadtest/chaos-upstream.js`).

| Field | Value |
| ----- | ----- |
| Executed | _to fill_ |
| Time to open (from first failure) | |
| Open-period failure P99 | |
| Recovery (open → closed) | |

### 2. Stream cut mid-flight → honest termination

- Inject: `X-Mockllm-Stream-Mode: abort` on the upstream request path.
- Observe: the client keeps the chunks already delivered, then receives
  exactly one `event: error` frame followed by `data: [DONE]`; the
  `breakwater_sse_stream_aborted_total` counter increments once per aborted
  stream.

| Field | Value |
| ----- | ----- |
| Executed | _to fill_ |
| Abort codes observed | |

### 3. Redis killed under load → fail-closed degradation

- Inject: stop Redis mid-run (`loadtest/redis-kill.js`).
- Observe: requests that cannot be rate-limited are rejected 503
  (fail-closed, never silently passed), the process-local response
  cache keeps serving its hits throughout because it never depended on
  Redis, readiness flips to 503; on Redis recovery the gateway
  returns to serving without restart. Quota leases survive in Redis and
  reconcile after recovery.

| Field | Value |
| ----- | ----- |
| Executed | _to fill_ |
| Rejections during outage | |
| Recovery time after restart | |
| Balance drift after recovery (must be 0) | |

### 4. Process killed between reserve and settle

- Inject: `SIGKILL` the gateway after reserves are visible in Redis but
  before settlement (scripted run with a large completion stream).
- Observe: the leases stay RESERVED until their own TTL elapses — the
  default reclaim horizon is the request budget plus a minute, so nothing
  is reclaimed at restart time itself. The sweeper then marks the orphaned
  leases EXPIRED and refunds their reservations in batches, and the
  balance identity reconciles from the next snapshot interval on.

| Field | Value |
| ----- | ----- |
| Executed | _to fill_ |
| Leases reclaimed | |
| Refund correctness | |
