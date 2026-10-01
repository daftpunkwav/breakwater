# internal/mockllm/ agent rules

Shared rules: [../AGENTS.md](../AGENTS.md). Package map: [README.md](README.md).
CLI wrapper: [../../cmd/mockllm/AGENTS.md](../../cmd/mockllm/AGENTS.md).

- The happy path is OpenAI Chat Completions, JSON and SSE. Gateway
  packages do not special-case this handler.
- Completions are deterministic. One token is one whitespace-separated
  word from the fixed vocabulary. The default length is 32 tokens.
  `X-Mockllm-Completion-Tokens` is capped at 100000.
- A malformed or out-of-range `X-Mockllm-*` header returns 400
  `invalid_fault_directive`.
- Apply faults in order: injected delay, process-wide error rate,
  per-request status override.
- The injected delay observes context cancellation.
- `StreamMode=abort` panics with `http.ErrAbortHandler`. Send no error
  envelope and no `[DONE]`.
- `StreamMode=slow` without a chunk delay uses 100ms.
- Cap request bodies at 1 MiB.
- Serve `POST /v1/chat/completions` and `GET /healthz`. A wrong method
  is 405. An unknown path is 404.
- In-process tests construct `Handler` directly. `cmd/mockllm` adds
  flags and the process lifecycle.
