# Breakwater

An LLM gateway written in Go that serves three client-facing API formats —
**OpenAI Chat Completions** (`/v1/chat/completions`), **OpenAI Responses**
(`/v1/responses`) and **Anthropic Messages** (`/v1/messages`) — over one
canonical governance pipeline, built to prove one thesis: high-concurrency
reliability governance — rate limiting, circuit breaking with failover,
bounded retry, cache stampede protection, lease-based quota consistency and
asynchronous observability — implemented by hand and backed by reproducible
tests, metrics and fault-injection experiments.

## Components

| Path                  | Responsibility                                            |
| --------------------- | --------------------------------------------------------- |
| `cmd/breakwater`      | Gateway binary (composition root)                         |
| `cmd/mockllm`         | Mock OpenAI-compatible upstream with fault injection      |
| `internal/server`     | Route assembly, the three inference endpoints, admin API (operations + identity administration) |
| `internal/relay`      | Response-side execution engine: attempts, failover, SSE passthrough, honest stream termination |
| `internal/pipeline`   | Middleware chain, per-request carrier, model authorization, observation stage |
| `internal/httpserver` | Shared HTTP lifecycle and the response tee                |
| `internal/insights`   | Monitoring record store: batched async writes, stability aggregation (success rate, failure mix, percentiles, timelines) |
| `internal/protocol`   | Wire contracts: the canonical chat form, the translator wires (openai-chat passthrough, openai-responses and anthropic-messages transcoding), SSE codecs |
| `internal/auth`       | Identity: users, roles, layered key limits (static/PostgreSQL stores, process-local LRU) |
| `internal/limiter`    | RPM/TPM token buckets (in-memory + Redis Lua), per-tenant concurrency gate, 429 stages |
| `internal/quota`      | Lease ledger (in-memory + Redis Lua), sweeper, 402 stage  |
| `internal/cache`      | Exact-match cache, hand-written singleflight, eligibility |
| `internal/circuit`    | Three-state breaker (plus a nop for breaker-less runs)    |
| `internal/retry`      | Attempt loop, budgets, retryability classifier            |
| `internal/router`     | Candidate selection: static priority or measured-latency order, breaker pre-filtering, runtime operator switches |
| `internal/upstream`   | Provider port + OpenAI-compatible adapter                 |
| `internal/config`     | Configuration schema and loading                          |
| `internal/obs`        | Bounded async access log, hand-written metrics registry   |
| `deploy`              | docker-compose stack, schema, seed, container build       |
| `loadtest`            | k6 scenarios, each targeting a system invariant           |
| `docs`                | Evidence documents (benchmarks, chaos report)             |

## Layout zoning

The directory tree is closed: future growth lands as new files inside
existing packages, never as new top-level directories. The zoning rules:

1. Governance mechanisms are self-contained: implementations, Lua
   scripts, sweepers and their own pipeline middleware grow inside their
   package — never subpackages.
2. Provider adapters stay flat: a new provider is a new file in
   `internal/upstream` (`openai.go`, `deepseek.go`), not an adaptor tree.
3. Everything HTTP-endpoint-shaped belongs to `internal/server`
   (business `/v1/*`, `/admin/*`, probes); everything wire-format-shaped
   belongs to `internal/protocol`.
4. `internal/httpserver` hosts only neutral, stdlib-only transport
   mechanics; composition happens exclusively in `cmd/*` roots.

## Client formats

| Route                      | Format                | Auth header              | Notes |
| -------------------------- | --------------------- | ------------------------ | ----- |
| `POST /v1/chat/completions`| OpenAI Chat (canonical)| `Authorization: Bearer` | Byte passthrough to OpenAI-compatible upstreams; SSE passthrough |
| `POST /v1/responses`       | OpenAI Responses      | `Authorization: Bearer`  | Translated: input/instructions/max_output_tokens in, Responses objects and `response.*` events out |
| `POST /v1/messages`        | Anthropic Messages    | `x-api-key` or Bearer    | Translated: system/blocks/required `max_tokens` in, Messages objects and `message_*` events out; `stop_sequences` refused rather than silently dropped |

Every response carries an `X-Request-Id` header: a well-formed
client-supplied id is adopted verbatim, otherwise one is minted
(`req-` prefix). The id travels to the upstream exchange and into the
access log, so one identifier joins the client-visible outcome, the
gateway's log line and the provider's records.

### Model names and aliases

The gateway routes on the model name the client sends. A `models`
entry of the form `"client=real"` serves the client-facing name by
forwarding the provider-real name — the adapter rewrites the request
body's `model` field, everything else passes through verbatim:

    "models": ["claude-sonnet=claude-sonnet-4-20250514", "deepseek-chat"]

With the same client-facing name bound to several upstreams, one
request carries its own failover chain — and because each upstream
rewrites to its own real name, a mid-request failover can cross
providers and models, not just hosts.

All three walk the identical governance pipeline (auth → model
authorization → concurrency → rate limit → quota → cache) and the identical relay engine; only the wire differs.
The canonical wire is openai-chat: unknown request fields survive byte
passthrough, while translated formats ingest a declared subset and
reject what they cannot express honestly. Text content only — image or
tool blocks are rejected at ingest. The exact-match cache serves the
canonical wire (translated replays would need response re-rendering,
which is deliberately not faked).

## Quick start

Start the mock upstream and the gateway with the in-memory governance
backends (no Redis needed for a smoke run):

    go run ./cmd/mockllm -addr 127.0.0.1:8090

    BREAKWATER_UPSTREAMS='[{"id":"mock","base_url":"http://127.0.0.1:8090","probe_url":"http://127.0.0.1:8090/healthz","models":["*"]}]' \
    BREAKWATER_IDENTITY='{"tiers":[{"id":"free","rpm":60,"tpm":200000,"max_tokens":4096,"monthly_quota":10000000,"allowed_models":["*"]}],"tenants":[{"id":"local","name":"Local","tier":"free","keys":["bw-local-dev-key"]}]}' \
    go run ./cmd/breakwater

Send a completion through the gateway:

    curl -s http://127.0.0.1:8080/v1/chat/completions \
      -H 'Authorization: Bearer bw-local-dev-key' \
      -H 'Content-Type: application/json' \
      -d '{"model":"mock-gpt","messages":[{"role":"user","content":"hi"}]}'

Stream one:

    curl -s -N http://127.0.0.1:8080/v1/chat/completions \
      -H 'Authorization: Bearer bw-local-dev-key' \
      -H 'Content-Type: application/json' \
      -d '{"model":"mock-gpt","stream":true,"messages":[{"role":"user","content":"hi"}]}'

With Redis configured (`BREAKWATER_REDIS_ADDR`), the limiter and quota
ledger run on atomic Lua scripts and the identity tier balances are
seeded automatically from the static identity set. With
`BREAKWATER_POSTGRES_DSN` configured, API keys resolve from PostgreSQL
(schema in `deploy/schema.sql`, local seed in `deploy/seed.sql`),
otherwise the `BREAKWATER_IDENTITY` JSON is the system of record.

## Configuration

All configuration is environment-based; core knobs:

| Variable                              | Default        | Effect                                                     |
| ------------------------------------- | -------------- | ---------------------------------------------------------- |
| `BREAKWATER_ADDR`                     | `:8080`        | Listen address                                             |
| `BREAKWATER_UPSTREAMS`                | _(none)_       | JSON list of upstreams (`id`, `base_url`, `probe_url`, `api_key`, `models`; list order = failover priority; `"client=real"` entries alias model names) |
| `BREAKWATER_ROUTING_STRATEGY`         | `static`       | Candidate order: `static` (configured order) or `latency` (measured exchange latency first, configured order as tie-break; untried upstreams are explored first) |
| `BREAKWATER_IDENTITY`                 | _(none)_       | JSON identity set (`tiers`, `tenants` with `role` and user-level `overrides`); arms the governance pipeline |
| `BREAKWATER_POSTGRES_DSN`             | _(none)_       | Identity system of record (overrides the static set)       |
| `BREAKWATER_REDIS_ADDR`               | _(none)_       | Enables the Redis backends; without it, in-memory          |
| `BREAKWATER_RETRY_MAX_ATTEMPTS`       | `3`            | Upstream attempts per request                              |
| `BREAKWATER_RETRY_BUDGET_MAX_IN_FLIGHT` | `64`         | Process-wide concurrent retry cap                          |
| `BREAKWATER_STREAM_TIMEOUT`           | `10m`          | Ceiling of a committed stream's body (after the headers); `0` lets the client own the stream's lifetime |
| `BREAKWATER_CACHE_ENABLED` / `_TTL` / `_CAPACITY` | on / `60s` / `1024` | Exact-match response cache      |
| `BREAKWATER_CIRCUIT_*`                | on / `5` / `30s` / `5s` | Breaker threshold, cooldown, probe timeout      |
| `BREAKWATER_ACCESS_LOG_PATH`          | _(off)_        | JSONL access log file (bounded queue, drop-oldest)         |
| `BREAKWATER_ADMIN_TOKEN`              | _(none)_       | Bearer token guarding `/admin/*` (empty = open, dev only)  |
| `BREAKWATER_RECONCILE_INTERVAL`       | `1m`           | Quota ledger reconciliation pacing (PRD Q6); needs Redis + PostgreSQL; `0` disables |
| `BREAKWATER_INSIGHTS_DSN`             | _(main DSN)_   | PostgreSQL the monitoring records persist to; defaults to `BREAKWATER_POSTGRES_DSN`; unset without any DSN |

## Operations surface

- `GET /healthz` — liveness
- `GET /readyz` — readiness (gates on the backend the fail-closed
  limiter depends on; it never lies)
- `GET /metrics` — Prometheus text exposition (hand-written registry)
- `GET /version` — build identifier and Go version
- `GET /admin/tenants/{id}/quota` — current balance
- `PUT /admin/tenants/{id}/quota` — top-up or correct a balance (`{"balance": N}`); the reconcile protocol treats the interval across a correction as skip-by-design
- `GET /admin/breakers` — per-upstream breaker states
- `GET /admin/insights?hours=N` — the stability report for the trailing window (default 24): success rate, failure mix by cause, latency percentiles, a 5-minute timeline and per-tenant/key/model/upstream breakdowns. Requires the monitoring store (any PostgreSQL DSN).
- `GET /admin/routing` — every known model and upstream with its current eligibility
- `PUT /admin/models/{id}` — enable or disable a model (`{"enabled": false}`); disabled models refuse requests with `403 model_disabled`
- `PUT /admin/upstreams/{id}` — enable or disable an upstream; disabled upstreams drop out of every candidate list. Switches are in-memory and reset on restart.

### Identity administration (PostgreSQL deployments)

With `BREAKWATER_POSTGRES_DSN` configured, identities are users with
keys — up to five per user — and each level carries its own limits
layer:

- `POST /admin/users` — create a user (`{"name", "tier", "role"}`; role `user` or `admin`)
- `GET /admin/users` — list users
- `PUT /admin/users/{id}/limits` — the user-level limits layer, applied to every key the user holds
- `POST /admin/users/{id}/keys` — issue a key (raw secret shown once)
- `GET /admin/users/{id}/keys` — list the user's keys
- `PUT /admin/keys/{id}/limits` — the key-level limits layer
- `PUT /admin/keys/{id}/status` — disable or re-enable one key (`{"enabled": false}`)

The limits layer (`LIMITS` below) is one JSON document:

```json
{"rpm": 30, "tpm": 50000, "max_tokens": 2048, "monthly_quota": 1000000,
 "concurrency": 4,
 "allowed_models": ["m1", "m2"],
 "denied_models": ["expensive-model"],
 "model_quotas": {"expensive-model": 100000}}
```

Resolution merges the tier template, the user layer and the key layer:
scalars take the nearest set value (key over user over tier);
`denied_models` union across layers — a user-level deny removes the
model from every key; `allowed_models` only ever tightens (an
`*`-wildcard tier plus a named user list means exactly that list);
`model_quotas` merge per model. An omitted field inherits; `{}` clears
the layer. Changes surface to live traffic within the auth cache TTL.
The static `BREAKWATER_IDENTITY` mode supports `role` and a
tenant-level `overrides` document, but has no administration surface.

## Fault injection interface

Per-request directives via headers:

| Header                        | Effect                                                     |
| ----------------------------- | ---------------------------------------------------------- |
| `X-Mockllm-Delay-Ms`          | Delay before the first response byte                       |
| `X-Mockllm-Status`            | Respond with this error status (400..599)                  |
| `X-Mockllm-Stream-Mode`       | `normal` (default) / `slow` / `abort`                      |
| `X-Mockllm-Chunk-Delay-Ms`    | Inter-chunk delay in `slow` mode                           |
| `X-Mockllm-Omit-Usage`        | Drop the usage field from responses                        |
| `X-Mockllm-Completion-Tokens` | Fixed completion length in tokens (words)                  |

Process-wide chaos knobs: `-default-delay`, `-error-rate`.

Example: a stream that dies mid-flight — the gateway keeps the delivered
chunks and terminates honestly with one in-stream error event and
`[DONE]`:

    curl -s -N http://127.0.0.1:8090/v1/chat/completions \
      -H 'X-Mockllm-Stream-Mode: abort' \
      -H 'Content-Type: application/json' \
      -d '{"model":"mock-gpt","stream":true,"messages":[{"role":"user","content":"hi"}]}'

## Developer tasks

A Makefile wraps the common loop (`make test`, `make race`, `make
cover`, `make lint`, `make build`, `make up`). `make build` injects the
git-derived version into the binaries.

## Local stack

    docker compose -f deploy/docker-compose.yml up -d

Starts Redis, PostgreSQL (identity schema and local seed applied on
first boot), the mock upstream and the gateway itself. Send a request
through the composed gateway:

    curl -s http://127.0.0.1:8080/v1/chat/completions \
      -H 'Authorization: Bearer bw-local-dev-key' \
      -H 'Content-Type: application/json' \
      -d '{"model":"mock-gpt","messages":[{"role":"user","content":"hi"}]}'

## Quality gate

CI enforces `gofmt`, `go vet`, `golangci-lint` and `go test -race ./...`
(with Redis and PostgreSQL service containers for the Lua and identity
integration tests). Statement coverage is held at or above 95% per
package — the taxonomy (unit / functional / integration / concurrency /
benchmark) and the naming rules live in `docs/TESTING.md`. Reliability
mechanisms (token bucket, circuit breaker, retry, singleflight) are
implemented in this repository by discipline; third-party governance
libraries are rejected by lint rule.

## Documentation

Evidence methodology and scenario sets live in `docs/`:
`docs/BENCHMARK.md` (load test numbers), `docs/CHAOS-REPORT.md`
(fault-injection timelines) and `docs/DEPLOY-LOCAL.md` (wiring real
providers and agent applications into a local gateway). BENCHMARK and
CHAOS-REPORT are filled from real runs of the `loadtest/` scenarios —
see `loadtest/README.md`.
