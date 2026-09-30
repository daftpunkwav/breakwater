# cache/

> 语言：**简体中文** | [English](README.md)

推理链的最后一级（auth → model authorization → concurrency → limiter →
quota → cache）：面向 canonical openai-chat wire 的精确匹配响应缓存。命中时
重放缓存的响应并将 `carrier.Consumed` 置零——这正是外围阶段全额退款的依据：
limiter 退还整笔预留，quota 阶段取消 lease 而非结算。缓存是优化手段，绝不
是正确性依赖：存储缺失或故障时退化为直接转发上游。

## Files

| File | 职责 |
| --- | --- |
| `cache.go` | 契约：存储条目 `Entry` 与 `Cache` port（`Get`/`Set` 携带基础 TTL；过期 jitter 由实现层叠加） |
| `key.go` | `KeyFor`：请求体原始字节的 SHA-256——匹配按字节精确，JSON key 顺序、空格或 SDK 的额外空白都构成不同 key |
| `eligibility.go` | `Eligible`：只有显式确定的参数组合才可缓存——`temperature` 显式为 `0`、`top_p` 缺省或恰为 `1`、`n` 恰为单选 |
| `memory.go` | `Memory`：进程内存储——容量上限加任意驱逐、±10% 的 TTL jitter 使对齐的过期无法聚集成风暴、每次 `Get` 返回私有副本 |
| `singleflight.go` | `Flight`：手写的 in-flight 去重（`x/sync/singleflight` 同类库被 `no-off-the-shelf-governance` lint 规则拒绝）；waiter 受自身 context 约束，共享 holder 的结果——错误也不例外 |
| `middleware.go` | pipeline 阶段本体：命中即重放、冷的非流式 key 走单次共享 fetch、流式请求各自 fetch 并在完成后尝试入库、上游自身产生的失败做短暂负缓存 |

## Scope rules

- 只有通过 `Eligible` 的 canonical wire 请求（`carrier.Format ==
  protocol.FormatOpenAIChat`）才会被缓存；translated format 绕过本阶段，因为
  重放需要把存储的响应重新渲染成客户端形态，这是刻意不做的事。
- 流式请求从不共享 flight：各自经 buffering tee 独立 fetch，完成后尝试入库，
  后写者覆盖。
- 共享 fetch 的生命周期不归属于碰巧发起它的那个请求：其 context 与该客户端的
  取消解绑，并重新以自己的 `fetchBudget` 封顶——发起者中途离场，既不会取消
  上游调用，也不会拖垮所有 waiter。
- 入库内容：完整 2xx 条目按基础 TTL 存储（超过 `maxCacheableBytes`（8 MiB）
  的响应仍送达客户端但绝不存储）；负条目按 TTL/10 短暂存储，且仅限上游自身
  产生的错误（400–507）或空成功。网关信封（circuit open、budget exhausted、
  unreachable）是瞬态而非事实，永不具备资格。
- 准入门只在容量压力下做决定：未满的缓存放行一切，驻留 key 的更新
  必定落地。门只存 key 哈希——读取（命中与未命中）喂养其频率证据，
  写入从不。
- 过期条目通过节流清扫停止占据容量，而非通过读取：`Get` 自行判断
  过期，清扫只是更早释放槽位。被清扫的 key 不向门喂任何信号——过期
  是时间事件，不是频率事件。
- 经 relay 错误事件契约终结的流（`relay.Result.Aborted`）永不入库：部分字节
  加错误帧不是可重放的完成态，无论状态码如何。

## Tests

`singleflight_bench_test.go` 基准钉住 flight group；其余覆盖所有权副本、
jitter 边界、eligibility 规则以及阶段的 hit / shared-fetch / stream 路径。
