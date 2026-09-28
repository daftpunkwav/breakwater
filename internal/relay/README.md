# relay/

> Language: **English** | [简体中文](README.zh.md)

The response-side execution engine. It turns one client request into exactly
one client-visible response: the attempt loop over candidate upstreams, the
failover order across candidates and fallback models, usage extraction, and
the honest termination of streaming replies. Candidate ordering belongs to
[`internal/router`](../router/), transport to the adapters in
[`internal/upstream`](../upstream/), wire formats to
[`internal/protocol`](../protocol/).

## Files

| File | Role |
| --- | --- |
| `executor.go` | `Executor`, `Job`, `Result`: the attempt loop driver; `finish` renders exactly one of the delivered reply, the passthrough of the last upstream error, or a gateway envelope; breaker grants and outcome reports happen per attempt; `Result.Trail` carries the per-attempt record for the access log |
| `exchange.go` | One upstream attempt: buffered mode (bodies bounded by `maxResponseBytes`, 32 MiB) and streaming mode (the commit point is the first byte written to the client; after it, failures terminate through the error event contract); a credential-class failure excludes the credential for the rest of the request and, while another credential remains alive, keeps the request walking instead of ending it |
| `fallback.go` | The fallback plan: model batches resolved lazily through `CandidateResolver` as the attempt loop exhausts them; the chain is a preference list, not a contract — an unresolvable model is skipped |
| `streamlease.go` | The per-attempt timer set of a streaming exchange: the attempt timeout bounds time-to-first-byte, the optional stream ceiling bounds the whole body, and the optional idle watchdog bounds upstream silence — every body read re-arms it |
| `stream.go` | The SSE passthrough pump: chunk-by-chunk with per-event flush, passive usage scraping, one reused outgoing buffer; the `[DONE]` sentinel separates a finished stream from a truncated one |
| `fatal.go` | `fatalUpstreamReason`: the deliberately narrow fatal classification (`insufficient_quota`, 401, credential-class 403) reported through the fatal hook with the credential that served the exchange — the assembly retires that credential and only a fully dead ring auto-disables the upstream |

## Tests

The attempt-loop branches (failover, fallback determinism, gateway error
rendering), the pump and its transcoder, the time budgets and the fatal
classification each have their own file; `stream_bench_test.go` benchmarks
the pump; `testupstream_test.go` is the shared fake upstream.

## Invariants

- Retry, circuit breaking and failover live inside the forward stage. The
  stages after it (settlement, cache write, observation) run once per client
  request, not per attempt — `Execute` returns a `Result` and never writes
  settlement itself.
- Exactly one HTTP response per job: a delivered reply, the verbatim
  passthrough of the last upstream error, or a gateway envelope.
- A committed stream never retries transparently: a mid-stream failure
  terminates through one error frame in the client's format; chunks already
  sent stay sent, and the loop sees `retry.ErrCommitted`.
- A client disconnect is nobody's fault: breaker accounting and the routing
  observer apply the same `clientFault` verdict wherever a disconnect
  surfaces, so walking away never demotes an upstream.
- A `Retry-After` hint delays only the next retry of the same credential
  and upstream; a hand-off to a different candidate, or to a different
  credential of the same upstream, never waits for the failed one's hint.
- The fatal classification stays narrow on purpose: an auto-disable takes
  real traffic down with it, so only deterministic failures (exhausted
  budget, rejected credentials) qualify — a request-scoped 403 never evicts
  an upstream for everyone. With a credential ring, the conviction lands on
  the credential first and the upstream only when its ring is empty.
- The attempt trail records every try, completed or not: which upstream,
  which credential, which status — a request's failover walk, not just its
  ending.
