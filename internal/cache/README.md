# cache/

> Language: **English** | [简体中文](README.zh.md)

The last stage of the inference chain (auth → model authorization →
concurrency → limiter → quota → cache): exact-match response caching for the
canonical openai-chat wire. A hit replays the stored response and zeroes
`carrier.Consumed`, which is what makes the surrounding stages refund in
full — the limiter returns the whole reservation and the quota stage cancels
the lease instead of settling it. The cache is an optimization, never a
correctness dependency: a missing or failing store degrades to direct
upstream forwarding.

## Files

| File | Role |
| --- | --- |
| `cache.go` | Contracts: the stored `Entry` and the `Cache` port (`Get`/`Set` with a base TTL; implementations add expiry jitter on top) |
| `key.go` | `KeyFor`: SHA-256 of the raw request body — matching is byte-exact, so JSON key order, spacing or SDK whitespace make different keys |
| `eligibility.go` | `Eligible`: only explicitly deterministic parameter combinations qualify — `temperature` explicitly `0`, `top_p` absent or exactly `1`, `n` exactly one choice |
| `memory.go` | `Memory`: the in-process store — capacity cap whose pressure decision runs through the admission gate, ±10% TTL jitter so aligned expiries cannot storm, a throttled in-write sweep that stops expired entries from occupying capacity, private copies per `Get` |
| `admission.go` | The TinyLFU-style admission gate: a count-min sketch (4 rows × 4-bit counters, saturating, periodically halved) behind a doorkeeper bloom absorbs every read — hits and misses alike — so one-shot keys estimate 1 and repeated keys accumulate. Under capacity pressure a new entry is admitted only when its estimate matches or beats the sampled victim's; updates of resident keys bypass the gate. The sketch stores hashes only, so the private-copy ownership contract is untouched |
| `singleflight.go` | `Flight`: hand-written in-flight dedup (the `x/sync/singleflight` equivalent is denied by the `no-off-the-shelf-governance` lint rule); waiters are bounded by their own context and share the holder's result, errors included |
| `middleware.go` | The pipeline stage: replay on hit, one shared fetch per cold non-streaming key, individual fetch plus post-completion store for streams, brief negative caching for upstream-produced failures |

## Scope rules

- Only canonical-wire requests (`carrier.Format == protocol.FormatOpenAIChat`)
  that pass `Eligible` are cached; translated formats bypass the stage, since
  replay would need response re-rendering, which is deliberately not faked.
- Streaming requests never share a flight: each fetches individually through
  a buffering tee and tries to store after completion, last write wins.
- The shared fetch outlives the request that happened to start it: its
  context is detached from that client's cancellation and re-bounded by its
  own `fetchBudget`, so a starter walking away mid-flight neither cancels the
  upstream call nor fails the waiters.
- Stored: full 2xx entries under the base TTL (responses above
  `maxCacheableBytes`, 8 MiB, reach the client but are never stored), and
  negative entries at TTL/10 only for upstream-produced errors (400–507) or
  empty successes. Gateway envelopes (circuit open, budget exhausted,
  unreachable) are transient states, never facts, and never qualify.
- The admission gate only decides under capacity pressure: a non-full
  cache admits everything, and an update of a resident key always
  lands. The gate stores key hashes only — reads (hits and misses)
  feed its frequency evidence, writes never do.
- Expired entries stop occupying capacity through the throttled sweep,
  not through reads: `Get` judges expiry itself and the sweep only
  frees the slots earlier. A swept key feeds nothing to the gate —
  expiry is a time event, not a frequency event.
- A stream the relay terminated through the error-event contract
  (`relay.Result.Aborted`) is never stored: partial bytes plus an error frame
  are not a replayable completion, whatever the status code says.

## Tests

The `singleflight_bench_test.go` benchmark pins the flight group; the rest
cover ownership copies, jitter bounds, eligibility rules and the stage's
hit/shared-fetch/stream paths.
