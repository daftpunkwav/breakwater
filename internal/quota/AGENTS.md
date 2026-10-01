# internal/quota/ agent rules

Shared rules: [../AGENTS.md](../AGENTS.md). Package map: [README.md](README.md).

- This package owns the balance ledger and lease lifecycle. Throughput
  windows belong to `internal/limiter`. Both reserve the pipeline's
  single token estimate.
- Stage placement: after the limiter, before the cache. Deny an
  estimate the balance cannot cover with 402 `insufficient_quota`.
- `Memory` and `Redis` implement the same ledger semantics. A behavior
  change updates both. Redis check, deduction, and lease insert are
  one Lua call. Write money as integer strings.
- A missing Redis balance key is denied. Do not create it on reserve.
- Every balance movement has one counter movement: debit at reserve,
  refund at settle or release. `Consumed` is not part of that identity.
- A terminal transition is one atomic state check plus refund. A
  sweeper and a late settle cannot both refund. A late settle after
  expiry is a no-op.
- Keep a terminal lease for `leaseAuditTTL` (1h), then purge it.
- Report reconcile drift. Do not write a correction from drift.
  `SetBalance` bumps the tenant epoch and the interval across that
  bump is skipped. `EnsureBalance` uses `SETNX`.
- The middleware reserves before `next` and settles or cancels in a
  `context.WithoutCancel` defer.
- `StartSweeper` waits for the first tick, then reclaims. Each pass
  requests at most `sweepBatch` (1000) reclaims. The composition root
  uses a 30s interval.
- `Memory.SweepOnce` scans at most `scanBatch` (8192) leases per walk.
- Reconciliation requires Redis and PostgreSQL. One round ends within
  its cadence, including when the store is wedged.
- Redis unit tests use miniredis. Snapshot integration tests require
  `BREAKWATER_TEST_POSTGRES_DSN`.
