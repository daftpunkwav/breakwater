# internal/

> Language: **English** | [简体中文](README.zh.md)

Every capability package of the gateway. Zoning rules that apply to all of
them (root README, "Layout zoning"):

1. Governance mechanisms are self-contained — implementations, Lua scripts,
   sweepers and their own pipeline middleware grow inside their package,
   never as subpackages.
2. Provider adapters stay flat: every OpenAI-compatible provider is served by
   `internal/upstream/openai.go` configured with a different `base_url`.
3. Everything HTTP-endpoint-shaped belongs to `internal/server`; everything
   wire-format-shaped belongs to `internal/protocol`. `internal/httpserver`
   hosts only neutral, stdlib-only transport mechanics.
4. Composition happens exclusively in the `cmd/*` roots.

| Package | Role | README |
| --- | --- | --- |
| [`auth/`](auth/) | Identity: users, roles, layered key limits (static/PostgreSQL stores, process-local LRU) | [link](auth/README.md) |
| [`cache/`](cache/) | Exact-match cache, hand-written singleflight, eligibility | [link](cache/README.md) |
| [`circuit/`](circuit/) | Breakers behind one port: three-state consecutive machine or windowed ratio guard (plus a nop for breaker-less runs) | [link](circuit/README.md) |
| [`config/`](config/) | Configuration schema and loading (`BREAKWATER_*` env → typed config) | — |
| [`httpserver/`](httpserver/) | Shared HTTP lifecycle and the response tee | — |
| [`insights/`](insights/) | Monitoring record store: batched async writes, stability aggregation (success rate, failure mix, percentiles, timelines) | — |
| [`limiter/`](limiter/) | RPM/TPM token buckets (in-memory + Redis Lua), per-tenant concurrency gate, 429 stages | [link](limiter/README.md) |
| [`mockllm/`](mockllm/) | The in-process mock upstream library with fault injection; `cmd/mockllm` is its CLI wrapper | [link](mockllm/README.md) |
| [`obs/`](obs/) | Bounded async access log, hand-written metrics registry | [link](obs/README.md) |
| [`pipeline/`](pipeline/) | Middleware chain, per-request carrier, model authorization, observation stage | [link](pipeline/README.md) |
| [`protocol/`](protocol/) | Wire contracts: the canonical chat form, the translator wires, SSE codecs | [link](protocol/README.md) |
| [`quota/`](quota/) | Lease ledger (in-memory + Redis Lua), sweeper, 402 stage | [link](quota/README.md) |
| [`relay/`](relay/) | Response-side execution engine: attempts, failover, SSE passthrough, honest stream termination | [link](relay/README.md) |
| [`retry/`](retry/) | Attempt loop, budgets, retryability classifier | [link](retry/README.md) |
| [`router/`](router/) | Candidate selection: static priority or measured-latency order, breaker pre-filtering, runtime operator switches | [link](router/README.md) |
| [`server/`](server/) | Route assembly, the three inference endpoints, admin API (operations + identity administration) | [link](server/README.md) |
| [`upstream/`](upstream/) | Provider port + OpenAI-compatible adapter | — |

Packages without a README are small leaves; their table row here is their
documentation. The inference chain order —
auth → model authorization → concurrency → limiter → quota → cache →
route → forward — passes through `pipeline`, `auth`, `limiter`, `quota`,
`cache`, `router` and `relay`, then renders through `protocol`.
