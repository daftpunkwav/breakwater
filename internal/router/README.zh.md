# router/

> 语言：**简体中文** | [English](README.md)

候选选择：把模型解析为有序的 upstream 实例，并预先剔除 breaker-open
与不具备资格的上游。本包拥有绑定解析、两种排序（静态配置顺序、按实测
延迟排序）、运行时操作员开关与延迟 tracker。它不执行 failover——relay
把候选列表当作 attempt 逐个走，`BREAKWATER_FALLBACKS` 链也是它自己
解析的（[fallback.go](../relay/fallback.go)）；它也绝不记账 breaker
状态，只通过 [`internal/circuit`](../circuit/) port 查询。inference
handler（[inference.go](../server/inference.go)）在 relay 启动前对每个
请求调用一次 `Candidates`。

## Files

| File | Role |
| --- | --- |
| `router.go` | `Router` port：`Candidates(ctx, model)` |
| `priority.go` | `Priority`：按配置顺序的绑定、`"*"` 通配、经 `StateOf` 的 breaker 预过滤、static/latency 排序与近平局首领抽取 |
| `control.go` | `Switch`：操作员对 model/upstream 的禁用、系统自动禁用（含原因与时刻）、`View` 快照 |
| `tracker.go` | `Strategy` 与 `ParseStrategy`；`Tracker`：每上游的 exchange-latency EWMA（alpha 0.25），每个连续失败附加 1000 ms 惩罚 |

## Invariants

- 预过滤是建议性的：`Candidates` 经 `StateOf` 剔除 breaker-open 条目，
  但执行与记账发生在 attempt 处——`Allow` 仍可能拒绝一个看起来合格的
  候选。
- 绑定顺序在构造后不可变；switch、tracker 与 breaker 状态都是叠加其上
  的运行时状态。
- 两条禁用通道互不越界：操作员的禁用只有操作员能解除；自动禁用（致命
  条件——凭证失效、配额耗尽）只有 recovery prober 能解除
  （[recovery.go](../../cmd/breakwater/recovery.go)）。操作员的 enable
  两条都清——人的意图赢。
- 未知名称读取时 fail-open（视为启用）、写入 setter 时 fail-closed
  （`ErrUnknownModel` / `ErrUnknownUpstream`），于是操作员的笔误既不能
  静默生效、也不能把流量锁在外面；wildcard 部署按需对具体名称开切换。
- tracker 只排序，从不剔除：未试过的上游得分为 0、被优先探索；快但在
  失败的上游会沉到较慢但健康的上游之下，直到第一次成功把惩罚完全
  清零。

错误映射是契约的一部分：`ErrDisabled` → 403（刻意的拒绝，不是健康
问题）、`ErrUnavailable`（有绑定但全部不合格）→ 503、完全无绑定 →
404。管理面：`GET /admin/routing`、`PUT /admin/models/{id}`、
`PUT /admin/upstreams/{id}`。配置：`BREAKWATER_ROUTING_STRATEGY=static|latency`。
