# loadtest

k6 load test and chaos scenarios for the breakwater gateway. Each
scenario targets a specific system invariant and produces the evidence
published under `docs/`.

## Prerequisites

- The gateway with governance armed (identity + Redis or memory mode)
- The mock upstream: `go run ./cmd/mockllm -addr 127.0.0.1:8090`
- [k6](https://k6.io) on PATH

Common environment: `BASE_URL` (default `http://127.0.0.1:8080`),`API_KEY` (a seeded key; each script defaults to `bw-local-t1`,`quota-race.js` to `bw-local-t2`), `RATE`, `DURATION`.`quota-race.js` additionally reads `ADMIN_TOKEN` for its teardown
query, which is required whenever the gateway runs with`BREAKWATER_ADMIN_TOKEN`.

## Scenarios

| Scenario       | Command                          | Verification target                                            |
| -------------- | -------------------------------- | -------------------------------------------------------------- |
| baseline       | `k6 run baseline.js`             | Pure forwarding QPS/P99, gateway overhead                      |
| cache-hit      | `k6 run cache-hit.js`            | Hit rate, hit vs fetch latency, one cold fetch per key         |
| rate-limit     | `k6 run rate-limit.js`           | 100% of over-limit requests get 429 + `Retry-After`, hard upstream ceiling |
| quota-race     | `k6 run quota-race.js`           | Concurrent drains reconcile to zero error                      |
| chaos-upstream | see file header                  | Breaker timeline, bounded failure latency                      |
| retry-storm    | see file header                  | Bounded retry amplification                                    |
| redis-kill     | see file header                  | Fail-closed degradation and recovery                           |

`chaos-upstream`, `retry-storm` and `redis-kill` require restarting the
mock upstream or Redis with specific fault settings mid-experiment; the
exact procedure is documented in each file's header.

## Reading the results

Gateway-side counters come from the scrape endpoint:

    curl -s $BASE_URL/metrics

The counters each scenario asserts on are the `breakwater_*` families
registered in `internal/obs/metrics.go`; every scenario's own file header names
the ones it reads. The published numbers are collected into
`docs/BENCHMARK.md` and `docs/CHAOS-REPORT.md`.
