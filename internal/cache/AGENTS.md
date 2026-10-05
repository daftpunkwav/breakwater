# internal/cache/ agent rules

Shared rules: [../AGENTS.md](../AGENTS.md). Package map: [README.md](README.md).

- Cache only canonical openai-chat requests (`protocol.FormatOpenAIChat`)
  that pass `Eligible`. Translated formats skip the stage.
- `Eligible` requires `temperature` present and exactly `0`. `top_p`
  may be absent or exactly `1`. `n` may be absent or exactly `1`.
- `KeyFor` is the SHA-256 of the raw request body. Matching is
  byte-exact.
- A hit replays the stored response and sets `carrier.Consumed` to zero.
  The replayed media type goes through `protocol.ForwardedContentType`:
  a store is an injected port, so its entry is not trusted to be one this
  gateway captured.
- A missing or failing store forwards to the upstream.
- Streaming requests do not share a flight. Each stream fetches on its
  own and may store after completion. Last write wins.
- Non-streaming cold keys share one flight. The shared fetch uses a
  context detached from the starter's cancellation and bounded by
  `fetchBudget`.
- `Flight` is the in-process singleflight. Do not import
  `golang.org/x/sync/singleflight`.
- Store full 2xx bodies at the base TTL when they are at most
  `maxCacheableBytes` (8 MiB). Larger 2xx bodies reach the client and
  are not stored.
- Negative caching is the non-streaming shared fetch. Store at `ttl/10`
  when `negativelyCacheable`: status 400–507, or an empty 2xx, and
  only when the relay names an upstream. Do not store gateway
  envelopes.
- Store a stream only when it is a complete 2xx within
  `maxCacheableBytes` and `Aborted` is false. Streams have no negative
  cache.
- `Get` returns a private copy. Under capacity pressure, admit a new
  key only when its frequency estimate matches or beats the sampled
  victim. Updates of resident keys skip the gate. A non-full cache
  admits every eligible entry.
- The frequency sketch stores hashes. Reads feed it. Writes do not.
  A swept expiry feeds nothing.
- Apply expiry jitter of ±10% on top of the base TTL.
