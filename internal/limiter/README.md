# limiter/

> Language: **English** | [简体中文](README.zh.md)

Time-window throughput protection: per-tenant RPM and TPM token buckets plus
the per-tenant concurrency gate. It is a pure mechanism — tenants appear only
as key material, the pipeline passes in the ceilings resolved from the
identity snapshot that [`internal/auth`](../auth/) produced. It does NOT own
the monetary balance ledger ([`internal/quota`](../quota/)); both stages
share the one token estimate this package's stage computes. Stage order:
auth → model authorization → concurrency → limiter → quota → cache; an
over-limit request is rejected with `429` and a `Retry-After` before any
upstream is touched.

Two `Limiter` backends: `Memory` (continuous-refill buckets, for tests and
the in-memory posture) and `Redis` (the same semantics executed atomically by
Lua on Redis `TIME`). The concurrency gate is a process-local counting
semaphore by design.

## Files

| File | Role |
| --- | --- |
| `limiter.go` | Contracts: `Limiter` (`Allow`/`Refund`), `Limits` (zero disables a ceiling), `Decision` (`Allowed`, `RetryAfter`) |
| `memory.go` | `Memory`: continuous-refill buckets that start full; `Refund` only raises a bucket toward capacity |
| `redis.go` | `Redis`: keys under `bw:limiter:` (or `<namespace>:bw:limiter:`), scripts embedded via `go:embed` |
| `tokenbucket.lua` | Atomic refill-check-deduct; Redis clock; capacity 0 skips the dimension; 120s key expiry |
| `refund.lua` | Refunds unconsumed tokens into the TPM bucket after a refill to now, never past capacity |
| `concurrency.go` | `Concurrency`: hand-written counting semaphore with an idempotent release |
| `middleware.go` | The 429 rate-limit stage: single body read, token estimate, fail-closed on backend error, refund-only post-call correction |
| `concurrencymiddleware.go` | The 429 concurrency stage: `concurrency_limit_exceeded`, taken before any rate-limit reservation |

## Tests

Bucket refill, edge cases and the middleware pipeline are unit-tested;
`redis_integration_test.go` needs Redis (`BREAKWATER_TEST_REDIS_ADDR`).

## Invariants

- A rejected request never reaches an upstream and never consumes quota: the
  concurrency gate runs before the rate-limit reservation, which runs before
  the quota lease.
- Backend errors fail closed: the middleware rejects with
  `503 governance_unavailable` instead of forwarding unmetered traffic. The
  fail-closed policy belongs to the pipeline layer, not the backends.
- Refund only, never surcharge: post-call correction returns what was not
  consumed; a refund can only return a reservation, never create allowance.
  If the client is gone the refund is lost — the safe direction.
- The Redis scripts clock from Redis `TIME`, never the application clock, so
  refill does not depend on a shared application clock.
- The concurrency gate's slots are process-local; a multi-instance deployment
  must not read it as a global ceiling.
