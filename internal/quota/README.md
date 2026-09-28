# quota/

> Language: **English** | [简体中文](README.zh.md)

The monetary balance ledger and the lease lifecycle. Each request reserves
its estimated token budget before the chain continues and settles against
actual consumption after it returns; a balance that cannot cover the estimate
is denied here with `402 insufficient_quota`. It does NOT own time-window
throughput — that is [`internal/limiter`](../limiter/); the token estimate is
computed once by the pipeline and shared through the request carrier. Stage
order: auth → model authorization → concurrency → limiter → quota → cache.

Two `Ledger` backends: `Memory` (reference semantics, tests, in-memory
posture) and `Redis` (the same semantics executed atomically by Lua, for
multi-instance deployments); the sweeper reclaims abandoned leases for both.
The reconciliation protocol (needs Redis + PostgreSQL, paced by
`BREAKWATER_RECONCILE_INTERVAL`) diffs snapshots against the balance identity.

## Files

| File | Role |
| --- | --- |
| `quota.go` | Contracts: `Ledger`, `Lease` (`RESERVED`/`SETTLED`/`EXPIRED`/`CANCELLED`), `ErrInsufficientBalance`, `ErrUnknownTenant` |
| `memory.go` | `Memory`: mutex-guarded reference implementation; `SweepOnce` scans at most `scanBatch` (8192) leases per pass |
| `redis.go` | `Redis`: balance/lease/sweep-zset/counter keys under `bw:quota:` (or `<namespace>:bw:quota:`), scripts embedded via `go:embed`, `TenantSnapshot` for reconcile |
| `reserve.lua` | Atomic balance check + deduction + lease record + debited-counter increment; a missing balance key is denied, never created |
| `settle.lua` | Atomic settle: refund (reserved − used) when positive, mark `SETTLED`, stamp the audit TTL |
| `release.lua` | Atomic cancel/expire: state check + full refund in one step, exactly once |
| `sweeper.go` | `StartSweeper`: periodic reclaim loop (first sweep after the first tick, pass limit 1000, 30s at the composition root) |
| `reconcile.go` | `Reconciler`/`Snapshot`: drift = Δdebited − Δrefunded − Δbalance; an epoch change skips the interval |
| `pgsnapshot.go` | `PGSnapshotStore`: the PostgreSQL `quota_snapshots` store, ordered by the append id sequence |
| `middleware.go` | The 402 stage: reserve before, settle-or-cancel after via a `context.WithoutCancel` defer |

## Tests

Redis ledger tests run over miniredis (no external Redis); the snapshot
integration test needs PostgreSQL (`BREAKWATER_TEST_POSTGRES_DSN`).

## Invariants

- No over-draft under concurrency: check, deduction and lease record are one
  Lua invocation; money values are written back through `%d` as integer
  strings.
- Every balance movement pairs with exactly one counter movement (debited at
  reserve, refunded at settle/release) — the reconcile identity. `Consumed`
  is observation only, deliberately outside the identity.
- Terminal transitions are idempotent and detectable: the state check and the
  refund are one atomic step, so a sweeper and a late settle can never both
  refund. A late settle after expiry is an expected, logged no-op.
- Terminal lease records are kept for the audit window (`leaseAuditTTL`, 1h)
  — a key TTL in Redis, dropped by the sweeper in memory — then purged.
- Drift is reported, never self-healed. An admin `SetBalance` bumps the
  tenant's epoch; the interval across the bump is skipped by design.
  `EnsureBalance` provisions with `SETNX`, so restarts never reset accounting.
