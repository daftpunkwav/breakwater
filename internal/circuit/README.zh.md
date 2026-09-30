# circuit/

> 语言：**简体中文** | [English](README.md)

手写的 per-upstream circuit breaker，在同一个 port 后面提供三种可互换
的策略：三态状态机（closed → open → half-open），对持续失败的上游切断
流量，并用恰好一个请求探测其恢复；ratio 守卫，按滚动结果窗口计算拒绝
概率，随失败占比上升拒绝越来越多的调用——面向"失败是比例式的"上游
（部分饱和的后端在拒绝一部分请求的同时仍成功另一部分），且永远不完全
切断流量；以及 slow-call 状态机，用退化证据驱动 consecutive 纪律——
窗口内慢完成占比达标即 open，面向"一直有应答但在散架"的上游。
本包只拥有这些状态机、它们的 port 和主动探测 helper——别无其他。什么算
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
| `breaker.go` | `Registry`：每个 upstream id 一份连续失败策略的进程内状态机——cooldown → 唯一的 half-open probe、结果记账、操作员 `Reset`、状态迁移观察者 |
| `ratio.go` | `RatioRegistry`：每个 upstream id 一份 ratio 策略守卫——10s 滚动窗口（40 × 250ms bucket），记录健康应答、上游故障与守卫自身产生的拒绝；`Allow` 按窗口计算拒绝概率（极小窗口受保护、尾部失败折扣过往成功、健康 bucket 稀释拒绝率），且每秒强制放行一个调用，恢复永远不需要操作员介入。`StateOf` 恒读 closed——守卫是一个概率，不是一个位置 |
| `slow.go` | `SlowRegistry`：每个 upstream id 一份 slow-call 策略状态机——以 {慢, 总数} 的 10s 滚动窗口驱动 consecutive 纪律；server fault 是最强的慢证据，样本不足的窗口永不开路，健康探测在清空后的窗口上关闭 breaker。breaker 从不自行判断快慢：慢完成以 `OutcomeSlow` 到达，由 relay 依照配置阈值分类（流式取首字节，其余取全程） |
| `nop.go` | `NopBreaker`：无条件放行、忘记一切结果；不持有任何状态 |
| `prober.go` | `ActiveProbe`：让一次合成健康检查走 breaker 的 Allow/Report 协议，恢复不必等待真实流量来充当探测 |

## Tests

`breaker_absorption_test.go` 覆盖迟到、重复与被遗弃的回报；
`breaker_defaults_test.go` 覆盖零值配置的默认替换；
`breaker_reset_test.go` 覆盖操作员 reset 路径；`ratio_test.go` 覆盖
ratio 守卫的保护下限、拒绝率伸缩、健康稀释、强制放行、窗口过期与
reset；`slow_test.go` 覆盖 slow-call 状态机的样本下限、比例触发、
故障即慢证据、探测恢复与 reset；`nop_test.go` 与 `prober_test.go`
各自覆盖自己的文件。

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
- ratio 策略：拒绝本身就是窗口里的事件，这正是失败持续期间拒绝概率
  得以维持的原因——每秒一次的强制放行是与之对冲的砝码，让恢复始终
  可被观察。没有任何近期事件的窗口放行一切；空闲时间不记仇。拒绝在
  `breakwater_circuit_denied_total` 指标上可见；状态 gauge 恒为
  closed，因为永远不会有状态迁移发生。

手写纪律：[.golangci.yml](../../.golangci.yml) 的
`no-off-the-shelf-governance` 规则拒绝 `github.com/sony/gobreaker`。
配置：`BREAKWATER_CIRCUIT_ENABLED` / `_STRATEGY`（`consecutive` 默认、
`ratio` 或 `slow-call`）/ `_FAIL_THRESHOLD` / `_COOLDOWN` /
`_PROBE_TIMEOUT` / `_SLOW_RATIO` / `_SLOW_THRESHOLD`（默认 on /
consecutive / 5 / 30s / 5s / 0.5 / 关；threshold 与 cooldown 驱动
consecutive 与 slow-call，ratio 对只驱动 slow-call）。慢完成对每个
策略都是健康证据：consecutive 与 ratio 状态机把 `OutcomeSlow` 与成功
完全同等吸收。操作面：`GET /admin/breakers` 与
`POST /admin/breakers/{id}/reset`（ratio 的 reset 清空窗口）。
