# internal/upstream/ agent rules

Shared rules: [../AGENTS.md](../AGENTS.md). Package row:
[../README.md](../README.md).

## Port

- Governance calls `Forward` for one exchange and the health probe.
  Nothing else on this package.
- A completed HTTP exchange, including 4xx and 5xx, returns a non-nil
  `Response` and a nil error. A non-nil error means the exchange did
  not complete.
- Do not classify retryability or breaker health here.
- The caller closes `Response.Body` and decodes it. Header values
  returned to callers are safe to retain.
- Retry, circuit breaking, and failover stay above this package.

## Adapter

- One file per concern: `upstream.go`, `openai.go`, `modelmap.go`,
  `transport.go`, `credential.go`. Do not add an adapter tree.
- Every OpenAI-compatible provider is an `OpenAIConfig` with its own
  `base_url`. Chat completions go to `/v1/chat/completions`.
- `ModelMap` rewrites a client-facing name. An unmapped name is
  forwarded unchanged.
- Skip indexes listed in `ExcludedCredentials`. Reuse an excluded
  credential only when every alive credential is excluded.
- A new provider is a new `Upstream` implementation. Do not branch on
  a vendor name in `relay`, `router`, or `pipeline`.
