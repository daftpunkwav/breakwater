# Chaos report

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

### 1. Upstream total failure → breaker timeline (I4)

- Inject: `mockllm -error-rate 1.0`
- Observe: consecutive failures open the breaker; client-perceived
  failure latency collapses from timeout-scale to fast 503; after the
  cooldown exactly one probe crosses; on recovery the state walks
  half-open → closed.
- Evidence: `circuit_open_total`, `circuit_half_open_total`,
  `circuit_state`, plus the k6 `failure_latency` trend
  (`loadtest/chaos-upstream.js`).

| Field | Value |
| ----- | ----- |
| Executed | _to fill_ |
| Time to open (from first failure) | |
| Open-period failure P99 | |
| Recovery (open → closed) | |

### 2. Stream cut mid-flight → honest termination (I6)

- Inject: `X-Mockllm-Stream-Mode: abort` on the upstream request path.
- Observe: the client keeps the chunks already delivered, then receives
  exactly one `event: error` frame followed by `data: [DONE]`; the
  `sse_stream_aborted_total` counter increments once per aborted
  stream.

| Field | Value |
| ----- | ----- |
| Executed | _to fill_ |
| Abort codes observed | |

### 3. Redis killed under load → fail-closed degradation (Q4 / I1/I3)

- Inject: stop Redis mid-run (`loadtest/redis-kill.js`).
- Observe: requests that cannot be rate-limited are rejected 503
  (fail-closed, never silently passed), the cache bypasses to the
  upstream, readiness flips to 503; on Redis recovery the gateway
  returns to serving without restart. Quota leases survive in Redis and
  reconcile after recovery.

| Field | Value |
| ----- | ----- |
| Executed | _to fill_ |
| Rejections during outage | |
| Recovery time after restart | |
| Balance drift after recovery (must be 0) | |

### 4. Process killed between reserve and settle (I9)

- Inject: `SIGKILL` the gateway after reserves are visible in Redis but
  before settlement (scripted run with a large completion stream).
- Observe: on restart, the sweeper marks the orphaned leases EXPIRED
  and refunds their reservations; the balance identity reconciles.

| Field | Value |
| ----- | ----- |
| Executed | _to fill_ |
| Leases reclaimed | |
| Refund correctness | |
