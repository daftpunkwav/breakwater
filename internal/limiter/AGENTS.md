# internal/limiter/ agent rules

Shared rules: [../AGENTS.md](../AGENTS.md). Package map: [README.md](README.md).

- This package owns RPM, TPM, and the per-tenant concurrency gate. It
  does not own the balance ledger (`internal/quota`).
- Tenants are key material. Ceilings come from the identity snapshot.
  This package does not load identity.
- `Memory` and `Redis` implement the same bucket semantics. A behavior
  change updates both. The Redis path stays in the embedded Lua and
  clocks from Redis `TIME`.
- A zero ceiling disables that dimension.
- `Refund` only moves a bucket toward capacity. It does not create
  allowance beyond the reservation.
- The 429 rate-limit stage runs after the concurrency stage and before
  quota. A rejection does not call an upstream and does not reserve
  quota. The code is `rate_limit_exceeded`. `Retry-After` is the wait
  in whole seconds, rounded up, and at least 1.
- A limiter backend error returns 503 `governance_unavailable`. The
  fail-closed decision lives in the middleware, not in `Memory` or
  `Redis`.
- The concurrency gate is a process-local counting semaphore with an
  idempotent release. It is not a cluster-wide ceiling.
- The gateway builds two gates from `Concurrency`: the per-tenant gate,
  and the relay per-upstream in-flight ceiling
  (`relay.WithUpstreamBulkhead`). The relay sees the second through its
  `Bulkhead` port.
- Over-capacity concurrency returns 429 `concurrency_limit_exceeded`
  before any rate-limit reservation.
- Post-call TPM correction calls `Refund` with the request context.
  `Redis.Refund` uses that context; a cancelled context fails the
  script and the middleware discards the error. `Memory.Refund`
  ignores the context and still returns tokens toward capacity. Do
  not move the Redis refund onto a detached context.
- Redis tests that need a real server require
  `BREAKWATER_TEST_REDIS_ADDR`. Unit tests use miniredis.
