# Breakwater

An OpenAI-compatible LLM gateway written in Go, built to prove one thesis:
high-concurrency reliability governance — rate limiting, circuit breaking
with failover, bounded retry, cache stampede protection, lease-based quota
consistency and asynchronous observability — implemented by hand and backed
by reproducible tests, metrics and fault-injection experiments.

> Work in progress. The repository currently contains the project skeleton:
> module layout, CI gate, local deployment stack and the mock upstream.

## Components

| Path                  | Responsibility                                            |
| --------------------- | --------------------------------------------------------- |
| `cmd/breakwater`      | Gateway binary                                            |
| `cmd/mockllm`         | Mock OpenAI-compatible upstream with fault injection      |
| `internal/server`     | Gateway route assembly                                    |
| `internal/httpserver` | Shared HTTP lifecycle (serve, drain, graceful shutdown)   |
| `internal/protocol`   | External wire contract: OpenAI schema, SSE codec          |
| `internal/relay`      | Response-side execution engine (reserved; lands with the forwarding milestone) |
| `internal/config`     | Configuration schema and loading                          |
| `internal/pipeline`   | Ordered middleware composition                            |
| `internal/obs`        | Access log contracts (bounded, never blocking)            |
| `internal/auth`       | API key to tenant contracts                               |
| `internal/limiter`    | RPM/TPM token bucket contracts                            |
| `internal/quota`      | Quota lease model contracts                               |
| `internal/cache`      | Exact-match cache contracts                               |
| `internal/circuit`    | Circuit breaker contracts                                 |
| `internal/retry`      | Attempt protocol and budget contracts                     |
| `internal/router`     | Upstream selection contracts                              |
| `internal/upstream`   | Provider port                                             |
| `deploy`              | docker-compose stack, schema, container build             |
| `loadtest`            | k6 scenarios (planned)                                    |

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

Placement of known future work:

| Future deliverable                         | Lands as                              |
| ------------------------------------------ | ------------------------------------- |
| `/v1/chat/completions` handler, admin API  | `server/completions.go`, `server/admin.go` |
| OpenAI schema, request normalization       | `protocol/schema.go`, `normalize.go`  |
| Response tee                               | `httpserver/tee.go`                   |
| OpenAI-compatible / DeepSeek adapters      | `upstream/openai.go`, `upstream/deepseek.go` |
| Rate limit backends + Lua                  | `limiter/memory.go`, `redis.go`, `tokenbucket.lua` |
| Lease ledger, sweeper, reconciliation      | `quota/ledger.go`, `sweeper.go`, `reconcile.go` |
| Breaker implementation + nop               | `circuit/breaker.go`, `nop.go`        |
| Attempt loop, failover engine              | `relay/executor.go`, `failover.go`    |
| Prometheus metrics                         | `obs/metrics.go`                      |
| Cost-weight routing, stream broadcaster    | `router/weight.go`, `cache/broadcast.go` |

## Quick start

Run the mock upstream:

    go run ./cmd/mockllm -addr 127.0.0.1:8090

Send a completion:

    curl -s http://127.0.0.1:8090/v1/chat/completions \
      -H 'Content-Type: application/json' \
      -d '{"model":"mock-gpt","messages":[{"role":"user","content":"hi"}]}'

Stream one:

    curl -s -N http://127.0.0.1:8090/v1/chat/completions \
      -H 'Content-Type: application/json' \
      -d '{"model":"mock-gpt","stream":true,"messages":[{"role":"user","content":"hi"}]}'

Run the gateway (health endpoints while the forwarding milestone is pending):

    go run ./cmd/breakwater

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

Example: a stream that dies mid-flight

    curl -s -N http://127.0.0.1:8090/v1/chat/completions \
      -H 'X-Mockllm-Stream-Mode: abort' \
      -H 'Content-Type: application/json' \
      -d '{"model":"mock-gpt","stream":true,"messages":[{"role":"user","content":"hi"}]}'

## Local stack

    docker compose -f deploy/docker-compose.yml up -d

Starts Redis, PostgreSQL (identity schema applied on first boot) and the
mock upstream.

## Quality gate

CI enforces `gofmt`, `go vet`, `golangci-lint` and `go test -race ./...`.
Reliability mechanisms (token bucket, circuit breaker, retry, singleflight)
are implemented in this repository by discipline; third-party governance
libraries are rejected by lint rule.

## Documentation

Public evidence documents (benchmarks, chaos reports) land under `docs/`
as milestones complete.
