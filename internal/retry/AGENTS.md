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
- `DefaultClassifier.Retryable` rejects nil and `context.Canceled`.
  It accepts `context.DeadlineExceeded`. A `StatusError` is accepted
  only for 429 or status >= 500. Every other error is accepted,
  including the relay's circuit-open sentinel.
- A positive `DelayHint` waits `jitteredHint`: uniform on
  `[hint, 1.5·hint)`, or the hint itself when half the hint is zero.
  `ParseRetryAfter` accepts an integer, a fractional number, or an
  HTTP date, and caps the hint at 60s.
- `StripRetryAfter` clears a hint. The relay calls it when the next
  attempt changes upstream id or rotates credential.
- Computed backoff is full-jitter exponential, bounded by the
  configured ceiling.
