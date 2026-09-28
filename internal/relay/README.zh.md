# relay/

> 语言：**简体中文** | [English](README.md)

响应侧执行引擎。它把一个客户端请求变成恰好一个客户端可见的响应：候选
upstream 上的 attempt loop、跨候选与 fallback 模型的 failover 顺序、usage
提取，以及流式回复的诚实终结。候选排序属于 [`internal/router`](../router/)，
传输属于 [`internal/upstream`](../upstream/) 的 adapter，wire 格式属于
[`internal/protocol`](../protocol/)。

## Files

| File | 职责 |
| --- | --- |
| `executor.go` | `Executor`、`Job`、`Result`：attempt loop 驱动器；`finish` 渲染三者之一——已交付的回复、最后一个上游错误的逐字节 passthrough、或网关信封；breaker 的许可与结果上报按 attempt 进行；`Result.Trail` 携带供访问日志使用的逐 attempt 记录 |
| `exchange.go` | 一次上游 attempt：buffered 模式（响应体受 `maxResponseBytes`（32 MiB）约束）与 streaming 模式（提交点是写向客户端的第一个字节；此后失败经错误事件契约终结）；credential 类失败会把该凭据排除出本次请求的剩余尝试——只要还有存活凭据，请求继续向下走而不是终止 |
| `fallback.go` | fallback 计划：attempt loop 耗尽当前模型后经 `CandidateResolver` 惰性解析下一批；链是偏好列表而非契约——无法服务的模型直接跳过 |
| `streamlease.go` | 流式 exchange 的每 attempt 计时器组：attempt timeout 约束 time-to-first-byte，可选的 stream ceiling 约束整个响应体，可选的 idle 看门狗约束上游静默——每次响应体读取都会重置它 |
| `stream.go` | SSE passthrough 泵：逐 chunk 转发、每事件 flush、被动刮取 usage、复用单个输出缓冲；`[DONE]` 哨兵区分正常完成与被截断的流 |
| `fatal.go` | `fatalUpstreamReason`：刻意收窄的 fatal 分类（`insufficient_quota`、401、credential 类 403），经 fatal hook 连同服务本次交换的凭据一并上报——assembly 先退役该凭据，环全部失效才 auto-disable 上游 |

## Tests

attempt loop 各分支（failover、fallback 确定性、gateway 错误渲染）、泵及其
transcoder、时间预算与 fatal 分类各有独立文件；`stream_bench_test.go` 为泵
做基准；`testupstream_test.go` 是共享的假 upstream。

## Invariants

- Retry、circuit breaking 与 failover 活在 forward stage 内部。其后的阶段
  （结算、cache 写入、observation）每客户端请求只跑一次，而非每 attempt 一次
  ——`Execute` 返回 `Result`，自己从不写结算。
- 每个 job 恰好一个 HTTP 响应：已交付的回复、最后一个上游错误的逐字节
  passthrough、或网关信封，三者必居其一。
- 已提交的流绝不透明重试：流中失败经一帧 client format 的错误帧终结；已发送
  的 chunk 保持已发送，loop 看到的是 `retry.ErrCommitted`。
- 客户端断连不是任何人的过错：breaker 记账与路由观察者在断连出现的任何位置
  应用同一个 `clientFault` 裁决——客户端离场永不降低某个 upstream 的评价。
- `Retry-After` 提示只推迟同一凭据、同一 upstream 的下一次重试；换候选、
  或换同一 upstream 的另一把凭据，绝不等待失败者的提示。
- fatal 分类刻意收窄：auto-disable 会连带砍掉真实流量，因此只有确定性失败
  （预算耗尽、凭据被拒）够格——请求级的 403 绝不把 upstream 从所有人面前
  逐出。有凭据环时，定罪先落在凭据上，环全部失效才轮到 upstream。
- attempt 轨迹记录每一次尝试，无论是否完成：哪个 upstream、哪把凭据、什么
  状态——这是一个请求走过的 failover 路径，而不只是它的结局。
