# mockllm/

> Language: **English** | [简体中文](README.zh.md)

The in-process mock OpenAI-compatible upstream used as the fault injector for the gateway's tests and experiments. The happy path stays OpenAI-chat shaped (non-streaming JSON and SSE chunk streams) so the gateway under test needs no special-casing, with faults layered on top. The CLI wrapper that turns this library into a process is [cmd/mockllm](../../cmd/mockllm/) (flags `-addr`, `-default-delay`, `-error-rate`); the two are related but distinct — tests embed the handler directly, the binary adds flags, signals and lifecycle.

## Files

| File | Role |
|---|---|
| `handler.go` | `Handler`/`Options`/`ServeHTTP`: routes `POST /v1/chat/completions` and `GET /healthz` (wrong method → 405, unknown path → 404), applies the process-wide knobs (`DefaultDelay`, `ErrorRate` → 500 `injected_failure`), caps request bodies at 1 MiB | |
| `faults.go` | The per-request `X-Mockllm-*` directives: delay, injected status (400..599), `StreamMode` (`normal`/`slow`/`abort`), chunk delay, usage omission, completion length. `slow` mode without an explicit chunk delay defaults to 100 ms |
| `completion.go` | OpenAI-compatible rendering: deterministic completions (one token = one whitespace-separated word from a fixed vocabulary; default 32 tokens, header-capped at 100000), the SSE chunk stream with its final usage chunk, the OpenAI error envelope |
| `health.go` | `GET /healthz` liveness probe for compose checks and experiment scripts |

## Invariants

- The happy path stays OpenAI-compatible: the mock must never force special-casing into the gateway.
- Content is deterministic on purpose — fixed vocabulary, fixed lengths — so load test runs stay comparable.
- Fault directives fail loudly: a malformed or out-of-range `X-Mockllm-*` header is `400 invalid_fault_directive`, because an experiment that silently ignored its own injection would produce meaningless evidence.
- `abort` mode cuts the connection mid-stream (panics with `http.ErrAbortHandler`) — no error envelope, no `[DONE]`: exactly the truncated stream the gateway's honest stream termination has to survive.
- Faults apply in a fixed order: the injected delay first (context-aware, so a client disconnect cuts it short), then the process-wide error rate, then the per-request status override.

The full fault-injection header table lives in the root [README](../../README.md); the scenarios that consume this mock live in [loadtest/](../../loadtest/).
