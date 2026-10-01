# internal/retry/ agent rules

Shared rules: [../AGENTS.md](../AGENTS.md). Package map: [README.md](README.md).

- This package owns the attempt loop, the in-flight budget, and the
  retryability classifier. It does not select candidates, account
  breaker state, or render the client response.
- `Execute` stops at `MaxAttempts`, `AttemptTimeout`, and
  `OverallDeadline`. `MaxAttempts < 1` clamps to 1. A zero timeout or
  deadline means the cap is absent.
- The first attempt does not take the budget. Every later attempt
  acquires it. An exhausted budget returns `ErrBudgetExhausted`
  wrapping the triggering error.
- `Budget` is the in-process counter: a fixed cap, or a share of the
  requests in flight floored at a minimum. Do not add a shared backend.
- Return `ErrCommitted` unchanged. Do not classify it and do not start
  another attempt.
- Do not retry `context.Canceled`. Retry timeouts and connection-level
  transport failures. Retry 429 and 5xx. Do not retry other 4xx.
- A `Retry-After` hint replaces computed backoff only for the next
  attempt on the same upstream. Callers strip it with `StripRetryAfter`
  before a different candidate. `ParseRetryAfter` accepts an integer,
  a fractional number, or an HTTP date, and caps the wait at 60s.
- Computed backoff is full-jitter exponential, bounded by the
  configured ceiling.
