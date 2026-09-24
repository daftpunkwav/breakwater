# Breakwater

An OpenAI-compatible LLM gateway written in Go, built to prove one thesis:
high-concurrency reliability governance — rate limiting, circuit breaking
with failover, bounded retry, cache stampede protection, lease-based quota
consistency and asynchronous observability — implemented by hand and backed
by reproducible tests, metrics and fault-injection experiments.

## Components

| Path                  | Responsibility                                            |
| --------------------- | --------------------------------------------------------- |
| `cmd/breakwater`      | Gateway binary (composition root)                         |
| `cmd/mockllm`         | Mock OpenAI-compatible upstream with fault injection      |
| `internal/server`     | Route assembly, completions endpoint, minimal admin API   |
| `internal/relay`      | Response-side execution engine: attempts, failover, SSE passthrough, honest stream termination |
| `internal/pipeline`   | Middleware chain, per-request carrier, observation stage  |
| `internal/httpserver` | Shared HTTP lifecycle and the response tee                |
| `internal/protocol`   | External wire contract: OpenAI schema, SSE error codec    |
| `internal/auth`       | Identity: static/PostgreSQL stores, process-local LRU     |
| `internal/limiter`    | RPM/TPM token buckets (in-memory + Redis Lua), 429 stage  |
| `internal/quota`      | Lease ledger (in-memory + Redis Lua), sweeper, 402 stage  |
| `internal/cache`      | Exact-match cache, hand-written singleflight, eligibility |
| `internal/circuit`    | Three-state breaker (plus a nop for breaker-less runs)    |
| `internal/retry`      | Attempt loop, budgets, retryability classifier            |
| `internal/router`     | Static priority routing with breaker pre-filtering        |
| `internal/upstream`   | Provider port + OpenAI-compatible adapter                 |
| `internal/config`     | Configuration schema and loading                          |
| `internal/obs`        | Bounded async access log, hand-written metrics registry   |
| `deploy`              | docker-compose stack, schema, seed, container build       |
| `loadtest`            | k6 scenarios, one per system invariant                    |
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
| `BREAKWATER_UPSTREAMS`                | _(none)_       | JSON list of upstreams (`id`, `base_url`, `probe_url`, `api_key`, `models`; list order = failover priority) |
| `BREAKWATER_IDENTITY`                 | _(none)_       | JSON identity set (`tiers`, `tenants`); arms the governance pipeline |
| `BREAKWATER_POSTGRES_DSN`             | _(none)_       | Identity system of record (overrides the static set)       |
| `BREAKWATER_REDIS_ADDR`               | _(none)_       | Enables the Redis backends; without it, in-memory          |
| `BREAKWATER_RETRY_MAX_ATTEMPTS`       | `3`            | Upstream attempts per request                              |
| `BREAKWATER_RETRY_BUDGET_MAX_IN_FLIGHT` | `64`         | Process-wide concurrent retry cap                          |
| `BREAKWATER_CACHE_ENABLED` / `_TTL` / `_CAPACITY` | on / `60s` / `1024` | Exact-match response cache      |
| `BREAKWATER_CIRCUIT_*`                | on / `5` / `30s` / `5s` | Breaker threshold, cooldown, probe timeout      |
| `BREAKWATER_ACCESS_LOG_PATH`          | _(off)_        | JSONL access log file (bounded queue, drop-oldest)         |
| `BREAKWATER_ADMIN_TOKEN`              | _(none)_       | Bearer token guarding `/admin/*` (empty = open, dev only)  |

## Operations surface

- `GET /healthz` — liveness
- `GET /readyz` — readiness (gates on the backend the fail-closed
  limiter depends on; it never lies)
- `GET /metrics` — Prometheus text exposition (hand-written registry)
- `GET /admin/tenants/{id}/quota` — current balance
- `GET /admin/breakers` — per-upstream breaker states

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

## Local stack

    docker compose -f deploy/docker-compose.yml up -d

Starts Redis, PostgreSQL (identity schema applied on first boot) and the
mock upstream.

## Quality gate

CI enforces `gofmt`, `go vet`, `golangci-lint` and `go test -race ./...`
(with Redis and PostgreSQL service containers for the Lua and identity
integration tests). Reliability mechanisms (token bucket, circuit
breaker, retry, singleflight) are implemented in this repository by
discipline; third-party governance libraries are rejected by lint rule.

## Documentation

Evidence methodology and scenario sets live in `docs/`:
`docs/BENCHMARK.md` (load test numbers) and `docs/CHAOS-REPORT.md`
(fault-injection timelines). They are filled from real runs of the
`loadtest/` scenarios — see `loadtest/README.md`.
