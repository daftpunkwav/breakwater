# loadtest

k6 load test and chaos scenarios for the breakwater gateway. Each
scenario targets a specific system invariant and produces the evidence
published under `docs/`.

## Prerequisites

- The gateway with governance armed (identity + Redis or memory mode)
- The mock upstream: `go run ./cmd/mockllm -addr 127.0.0.1:8090`
- [k6](https://k6.io) on PATH

Common environment: `BASE_URL` (default `http://127.0.0.1:8080`),
`API_KEY` (a seeded key), `RATE`, `DURATION`.

## Scenarios

| Scenario       | Command                          | Verification target                                            | Invariant |
| -------------- | -------------------------------- | -------------------------------------------------------------- | --------- |
| baseline       | `k6 run baseline.js`             | Pure forwarding QPS/P99, gateway overhead                      | -         |
| cache-hit      | `k6 run cache-hit.js`            | Hit rate, hit vs fetch latency, one cold fetch per key         | I2        |
| rate-limit     | `k6 run rate-limit.js`           | 100% of over-limit requests get 429 + `Retry-After`, hard upstream ceiling | I1 |
| quota-race     | `k6 run quota-race.js`           | Concurrent drains reconcile to zero error                      | I3/I9     |
| chaos-upstream | see file header                  | Breaker timeline, bounded failure latency                      | I4/I6     |
| retry-storm    | see file header                  | Bounded retry amplification                                    | I5        |
| redis-kill     | see file header                  | Fail-closed degradation and recovery                           | I1/I3     |

`chaos-upstream`, `retry-storm` and `redis-kill` require restarting the
mock upstream or Redis with specific fault settings mid-experiment; the
exact procedure is documented in each file's header.

## Reading the results

Gateway-side counters come from the scrape endpoint:

    curl -s $BASE_URL/metrics

The mechanism-to-metric mapping lives in the spec's observation table;
the published numbers are collected into `docs/BENCHMARK.md` and
`docs/CHAOS-REPORT.md`.
