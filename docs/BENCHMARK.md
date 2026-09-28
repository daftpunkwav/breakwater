# Benchmarks

> Language: **English** | [简体中文](BENCHMARK.zh.md)

> Status: methodology and scenario set are final; the numbers below are
> filled in by running the loadtest scenarios on real hardware. No
> figures on this page are estimates — an empty cell means "not yet
> measured on record", never "assumed".

## Method

- Load generator: k6 (`loadtest/`), constant-arrival-rate executors so
  offered load is independent of system response time.
- Upstream: `mockllm` with its default completion length of 32 words, so
  byte volume is comparable across runs. The mock also accepts
  `X-Mockllm-Completion-Tokens`, but the gateway forwards no client header
  to the upstream, so a gateway-mediated run always uses the default.
- Machine and process placement, Go version and Redis/PostgreSQL
  versions are recorded per run below.
- Latency is end-to-end client-side (k6 `http_req_duration`); gateway
  internals are read from `/metrics` afterwards, never mixed in.

## Environment (per run)

| Field        | Value |
| ------------ | ----- |
| Date         | _to fill_ |
| Machine      | _to fill_ |
| Go           | _to fill_ |
| Redis        | _to fill_ |
| Topology     | gateway and mockllm co-located / separated |

## Scenarios

| Scenario      | Offered rate | Throughput | P50 | P99 | Error profile |
| ------------- | ------------ | ---------- | --- | --- | ------------- |
| baseline      | _to fill_    |            |     |     |               |
| cache-hit     |              |            |     |     | hit rate:     |
| rate-limit    |              |            |     |     | 429 share, upstream ceiling: |
| quota-race    |              |            |     |     | reconciliation drift (must be 0): |
| chaos-upstream|              |            |     |     | breaker timeline: |
| retry-storm   |              |            |     |     | amplification factor: |
| redis-kill    |              |            |     |     | fail-closed behavior: |

## Gateway self-loss

Acceptance target: with a zero-delay upstream, the gateway's
P99 forwarding overhead should stay in single-digit milliseconds.

| Run | mockllm P99 direct | through gateway | delta |
| --- | ------------------ | --------------- | ----- |
|     |                    |                 |       |

## Reproducing

Each number above comes from one command in `loadtest/` (see the
per-file headers for the chaos variants). Environment knobs:
`BASE_URL`, `API_KEY`, `RATE`, `DURATION`.
