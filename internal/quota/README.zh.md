# quota/

> 语言：**简体中文** | [English](README.md)

余额账本与 lease 生命周期。每个请求在管线继续前预留估算 token 预算，返回后
按实际消耗结算；余额不足以覆盖估算时在此处以 `402 insufficient_quota`
拒绝。它不负责时间窗吞吐——那是 [`internal/limiter`](../limiter/) 的事；
token 估算值由管线计算一次，经请求 carrier 共享。阶段顺序：auth →
model authorization → concurrency → limiter → quota → cache。

`Ledger` 有两个后端：`Memory`（参照语义、测试、内存退化形态）和 `Redis`
（同一语义经 Lua 原子执行，面向多实例部署）；sweeper 对两者回收被遗弃的
lease。对账协议（需要 Redis + PostgreSQL，由 `BREAKWATER_RECONCILE_INTERVAL`
定速）将快照对余额恒等式做差。

## Files

| File | Role |
| --- | --- |
| `quota.go` | 契约：`Ledger`、`Lease`（`RESERVED`/`SETTLED`/`EXPIRED`/`CANCELLED`）、`ErrInsufficientBalance`、`ErrUnknownTenant` |
| `memory.go` | `Memory`：互斥锁保护的参照实现；`SweepOnce` 每趟至多扫 `scanBatch`（8192）条 lease |
| `redis.go` | `Redis`：`bw:quota:`（或 `<namespace>:bw:quota:`）下的 balance/lease/sweep zset/counter key，脚本经 `go:embed` 内嵌，`TenantSnapshot` 供对账 |
| `reserve.lua` | 原子地检查余额 + 扣减 + 写 lease 记录 + 递增 debited 计数器；balance key 缺失即拒绝，绝不凭空创建 |
| `settle.lua` | 原子结算：差额（reserved − used）为正则退还，标记 `SETTLED`，盖上审计 TTL |
| `release.lua` | 原子 cancel/expire：状态检查 + 全额退还一步完成，且只发生一次 |
| `sweeper.go` | `StartSweeper`：周期回收循环（首次回收在第一个 tick 之后，单趟上限 1000，组合根为 30s） |
| `reconcile.go` | `Reconciler`/`Snapshot`：drift = Δdebited − Δrefunded − Δbalance；epoch 变更则跳过该区间 |
| `pgsnapshot.go` | `PGSnapshots`：PostgreSQL `quota_snapshots` 存储，按 append id 序列排序 |
| `middleware.go` | 402 阶段：先预留，后经 `context.WithoutCancel` 的 defer 结算或取消 |

## Tests

Redis 账本测试跑在 miniredis 上（无需外部 Redis）；快照集成测试需要
PostgreSQL（`BREAKWATER_TEST_POSTGRES_DSN`）。

## Invariants

- 并发下绝不透支：检查、扣减与 lease 记录是一次 Lua 调用；金额写回一律经
  `%d` 成为整数字符串。
- 每一笔余额变动都恰好配对一次计数器变动（reserve 时 debited，
  settle/release 时 refunded）——即对账恒等式。`Consumed` 仅作观测，刻意
  在恒等式之外。
- 终态迁移幂等且可检测：状态检查与退款是一个原子步骤，sweeper 与迟到结算
  不可能双重退款。过期后的迟到结算是被预期的 no-op，并留有日志。
- 终态 lease 记录保留一个审计窗口（`leaseAuditTTL`，1h）——Redis 以 key
  TTL 实现，内存版由 sweeper 按同窗龄清除——之后清除。
- drift 只上报，绝不自愈。管理端 `SetBalance` 递增租户 epoch；跨越 bump 的
  区间按设计跳过。`EnsureBalance` 以 `SETNX` 播种，重启不会重置记账。
