# circuit/

> 语言：**简体中文** | [English](README.md)

手写的 per-upstream circuit breaker：一个三态状态机（closed → open →
half-open），对持续失败的上游切断流量，并用恰好一个请求探测其恢复。
本包只拥有状态机本身、它的 port 和主动探测 helper——别无其他。什么算
失败是调用方的策略，通过 `Outcome` 传入；候选预过滤属于
[`internal/router`](../router/)；每次 attempt 的授予与回报属于 relay
的 executor（[exchange.go](../relay/exchange.go)）；探测调度属于组装层
（[recovery.go](../../cmd/breakwater/recovery.go)）。
`BREAKWATER_CIRCUIT_ENABLED=false` 的部署组装的是 nop 实现，调用方
永远不需要对 nil 分支。

## Files

| File | Role |
| --- | --- |
| `circuit.go` | 契约：`State`（`StateClosed` / `StateOpen` / `StateHalfOpen`）、`Outcome`（Success / ClientFault / ServerFault / GatewayTerminated）、`Permission`、`Breaker` port（`Allow` / `StateOf` / `Reset`） |
| `breaker.go` | `Registry`：每个 upstream id 一份进程内状态机——cooldown → 唯一的 half-open probe、结果记账、操作员 `Reset`、状态迁移观察者 |
| `nop.go` | `NopBreaker`：无条件放行、忘记一切结果；不持有任何状态 |
| `prober.go` | `ActiveProbe`：让一次合成健康检查走 breaker 的 Allow/Report 协议，恢复不必等待真实流量来充当探测 |

## Tests

`breaker_absorption_test.go` 覆盖迟到、重复与被遗弃的回报；
`breaker_defaults_test.go` 覆盖零值配置的默认替换；
`breaker_reset_test.go` 覆盖操作员 reset 路径；`nop_test.go` 与
`prober_test.go` 各自覆盖自己的文件。

## Invariants

- open 状态拒绝一切调用，不触碰上游；half-open 只允许恰好一个在途
  probe——并发到达被直接拒绝，绝不排队。
- 被授予的调用恰好回报一次结果；重复回报与被遗弃的 permission 都被
  吸收。持有者永不回报的 probe，会在 probe 超时后的下一次 `Allow`、
  `StateOf` 或 `Report` 中被作为 server fault 回收——deadline 是唯一
  的恢复信号；没有任何机制直接感知 panic 或取消。
- `OutcomeGatewayTerminated` 不是健康证据：closed 保持失败计数原样，
  被网关截断的 half-open probe 带着全新 cooldown 回到 open——证明不了
  任何事的 probe 不得关闭 breaker。
- `StateOf` 执行惰性迁移（open cooldown 到期 → half-open），但绝不
  分配 probe 槽位；探测是 `Allow` 独占的事务，router 的读取永远不会
  消耗掉那次 half-open 机会。
- 状态按设计是进程本地的；port 保持可替换，以便日后接入共享后端。

手写纪律：[.golangci.yml](../../.golangci.yml) 的
`no-off-the-shelf-governance` 规则拒绝 `github.com/sony/gobreaker`。
配置：`BREAKWATER_CIRCUIT_ENABLED` / `_FAIL_THRESHOLD` / `_COOLDOWN` /
`_PROBE_TIMEOUT`（默认 on / 5 / 30s / 5s）。操作面：
`GET /admin/breakers` 与 `POST /admin/breakers/{id}/reset`。
