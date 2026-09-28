# pipeline/

> Language: **English** | [简体中文](README.zh.md)

The request-side governance pipeline: ordered middleware composition over
`net/http` handlers plus the per-request state they share. One chain is
composed per client format; the fixed order reads outward-in on the request
side and inward-out on the response side, since a stage that reserves before
`next()` settles after it returns:

    request side:  carrier -> request id -> format -> observation
                   -> auth -> model authorization -> concurrency
                   -> limiter -> quota(reserve) -> cache -> route -> forward
    response side: forward -> retry/circuit -> quota(settle) -> cache write -> obs

It owns the stage machinery only — limiter, quota, cache and relay are their
own packages that join as middleware or the terminal handler.

## Files

| File | Role |
| --- | --- |
| `chain.go` | `Chain`/`Middleware`: ordered composition, first listed stage outermost |
| `carrier.go` | The `Carrier`: the per-request typed struct (identity, parsed request, token estimate, lease and consumption handles) assembled once at chain entry; `RequireCarrier`, `SetBody` |
| `body.go` | `FormatStage` pins the client format; `EnsureBody` reads the bounded body exactly once (`protocol.MaxBodyBytes` cap) and ingests it through the format's wire |
| `requestid.go` | `RequestIDStage`: adopt a well-formed client `X-Request-Id` (8–128 printable ASCII) or mint a `req-` id; echoed on every response, rejections included |
| `obsmiddleware.go` | `ObservationStage`: in-flight gauge, duration histogram, outcome counters, the async access-log entry; the first stage to see the outcome of a finished business request, rejections included |
| `authstage.go` | `AuthStage`: Bearer / `x-api-key` → tenant through `auth.Store`; 401/503 rendered in the client format — a store outage surfaces as 503, never a silent pass |
| `authzstage.go` | `ModelAuthzStage` and `AuthorizeModel`: tier model authorization, placed after auth and before any governance spend and before the cache; the one authority the inference handler also calls, so the two verdicts cannot drift |
| `estimate.go` | `EstimateTokens` / `EstimatePartialTokens` / `PromptTokens`: the single pre-call estimate both the TPM bucket and the quota lease reserve against; a declared `max_tokens` is clamped to the tenant's per-request cap (the self-inflicted DoS guard) |
| `recovery.go` | `RecoveryStage`: a handler panic becomes a rendered 500 when the response has not started; innermost, immediately around the endpoint handler |

## Tests

One test file per stage (composition, body ingest, the failure taxonomy,
request-id adoption); `teststore_test.go` provides the shared fake auth
store.

## Invariants

- Retry, circuit breaking and failover must never be composed as Middleware:
  they wrap the upstream call inside the terminal forward stage (a retry
  cannot replay a half-consumed `http.Handler` response). They belong to
  [`internal/relay`](../relay/).
- Stages wrap `http.Handler` so `http.Flusher` implementations survive every
  layer — the prerequisite for SSE passthrough.
- The body is read exactly once per request into the carrier; transport-level
  rejections (too large, unreadable, malformed) are request facts rendered in
  the client format's envelope, not stage opinions.
- The carrier is a dumb typed struct on the single request goroutine — no
  locking, no grab-bag of context values; each field is written by the one
  stage that owns it.
- The model authorization rule exists once (`AuthorizeModel`); the pipeline
  stage and the inference handler deliberately duplicate the enforcement,
  never the rule.
