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
| `internal/server`     | HTTP lifecycle, routing, graceful shutdown                |
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
