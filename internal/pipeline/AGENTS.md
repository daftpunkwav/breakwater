# internal/pipeline/ agent rules

Shared rules: [../AGENTS.md](../AGENTS.md). Stage map: [README.md](README.md).

## Order

Outermost first:

`carrier`, `request id`, `format`, `observation`, `auth`, model
authorization, `concurrency`, `limiter`, `quota` reserve, `cache`.

The terminal handler routes and forwards. Cache is omitted when
caching is disabled. `RecoveryStage` is appended after governance, so
it is innermost and still inside observation.

A stage that reserves before `next` settles after `next` returns.

## Boundaries

- This package owns chain composition and the per-request `Carrier`.
  Limiter, quota, cache, and relay stay in their packages.
- Do not compose retry, circuit breaking, or failover as `Middleware`.
  They wrap the upstream call inside `internal/relay`.
- Middleware wraps `http.Handler` so an `http.Flusher` survives every
  layer.
- Read the body once, bounded by `protocol.MaxBodyBytes`, into the
  carrier. Render a transport rejection in the client format's
  envelope.
- The carrier is a typed struct used on the request goroutine. Do not
  add a lock. Do not store request fields in context values. One stage
  writes each field.
- `AuthorizeModel` is the only model-authorization rule. The pipeline
  stage and the inference handler both call it.
- `EstimateTokens` is the only pre-call estimate. The TPM bucket and
  the quota lease both reserve against it. Clamp a declared
  `max_tokens` to the tenant per-request cap.
- An auth-store outage is 503. Do not continue the chain.
- Adopt a client `X-Request-Id` of 8–128 printable ASCII, or mint a
  `req-` id. Echo it on every inference response, including rejections.
- `RecoveryStage` is the innermost stage around the endpoint handler.
  A panic before the response starts becomes a rendered 500.
