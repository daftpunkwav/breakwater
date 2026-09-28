# server/

> Language: **English** | [简体中文](README.zh.md)

Everything HTTP-endpoint-shaped: route assembly, the three inference
endpoints (openai-chat, openai-responses, anthropic-messages), the admin API
in its two halves (operations + identity administration), the probes and the
discovery endpoints. The listen/serve/drain lifecycle is delegated to
[`internal/httpserver`](../httpserver/) so every binary shares one shutdown
ordering. Wire-format concerns belong to
[`internal/protocol`](../protocol/), not here.

## Files

| File | Role |
| --- | --- |
| `routes.go` | `newRootHandler`: the explicit URL layout in one place; the whole mux is wrapped in an outer recovery stage so the non-inference routes also render a 500 instead of a killed connection |
| `server.go` | `Server`/`Options`: the gateway facade; delegates the process lifecycle to `httpserver.Run` |
| `inference.go` | `Inference`: one handler per client format — model authorization (defense in depth with the pipeline stage), router candidates, the context-window pre-filter (413 `context_window_exceeded`, fails open), the fallback resolver applying tier and context gates, relay execution and the settlement input |
| `admin.go` | The operations half of the admin surface: the bearer guard, quota read/top-up, breaker states and reset, the routing view, the model/upstream switches, the insights report |
| `identityadmin.go` | The identity half: the `/admin/users` and `/admin/keys` subtrees; `AdminStore` sentinel errors map onto 404/409/503 |
| `health.go` | Liveness and readiness; readiness gates on the injected probe — the fail-closed limiter's dependency — and keeps infrastructure details in the log, never in the body |
| `models.go` | `GET /v1/models`: the client-facing model names in the OpenAI list form; unauthenticated, carries no tenant data, rendered once at construction |

## Tests

One file per surface: the three inference formats and their guards, the
context filter, the admin operations and identity endpoints, the routing
switch endpoints, probes and version.

## Invariants

- Composition happens exclusively in the `cmd/*` roots; this package assembles
  routes but owns no wiring policy.
- The `/admin/` mux pattern is deliberately not method-qualified: the admin
  handler guards methods itself, so a method-qualified pattern would bounce
  PUT at the mux with a 405 the top-up endpoint could never see.
- Readiness must gate on the dependency whose absence flips the degradation
  posture — while rate limiting is fail-closed, a readiness that skips the
  backend lies. Probe failures log the detail and answer a stable phrase.
- The inference chains keep an inner recovery copy inside the observation
  stage: there a panic is counted as a 500 gateway fault, not a disconnect.
- `GET /v1/models` stays unauthenticated and tenant-free by design: discovery
  must not leak what the governance stages gate.
