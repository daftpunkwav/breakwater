# loadtest

k6 load test and chaos scenarios for the breakwater gateway. Scenarios land
together with the benchmark evidence milestone; each scenario targets a
specific system invariant:

| Scenario       | Verification target                                             | Invariant |
| -------------- | --------------------------------------------------------------- | --------- |
| baseline       | Pure forwarding QPS/P99, gateway overhead                        | -         |
| cache-hit      | Hit rate, hit vs fetch latency                                   | I2        |
| rate-limit     | 100% of over-limit requests get 429, hard upstream QPS ceiling   | I1        |
| quota-race     | Concurrent drains reconcile to zero error                        | I3/I9     |
| chaos-upstream | Breaker timeline, bounded failure latency                        | I4/I6     |
| retry-storm    | Bounded retry amplification                                      | I5        |
| redis-kill     | Degradation and recovery of the governance layer                 | I1/I3     |
