# obs/

> 语言：**简体中文** | [English](README.md)

网关的观测原语：有界异步 access log 与手写的 Prometheus 风格 metrics registry。本包只负责记录机制——存在哪些 sink、如何接线，由 `cmd/breakwater` 决定（combined fan-out、drop counter 发布），`/admin/insights` 背后的聚合则住在 `internal/insights`。

## Files

| 文件 | 职责 |
|---|---|
| `accesslog.go` | 契约：append-only 的 `Entry` schema（tenant/key/model/upstream 维度、status、duration、`CacheHit`、结算后的 `Tokens`、`ErrorCode`）、异步 `Sink` port，以及请求未产生响应就结束时的 `StatusClientClosedRequest`（499） |
| `logger.go` | `Logger`，JSONL sink：mutex 保护的 ring queue（不是 channel——drop-oldest 需要驱逐语义），由单个后台 goroutine 排空；溢出时丢最旧的并计数；`Dropped`/`Written`/`Buffered` 暴露账目；`Flush`/`Close` 仅用于 shutdown 路径 |
| `metrics.go` | `Metrics`：所有 `breakwater_*` metric family 配带类型的 recorder 方法，scrape 时渲染为 Prometheus text format 0.0.4——counter、gauge、两个按 upstream 的 histogram（`breakwater_request_duration_seconds`、`breakwater_upstream_ttft_seconds`；`defaultBuckets`，1 ms–60 s），外加标量 gauge `breakwater_inflight_requests` |

## Invariants

- `Record` 永不阻塞请求路径。容量压力以显式、被计数的 drop 结束——绝不反压，也绝不静默丢失。malformed entry 或写失败同样计入 drop counter，账目保持诚实。
- label 词汇有界：`tenant`、`model`、`upstream`、`status`（外加 failover 的 `from`/`to`、probe 的 `result`/`reason`）——没有 request ID，没有 path。`maxChildren`（4096）限制每个 family 的 label-set 叶子数；越过上限后，新集合坍缩进一个保留的 overflow 叶子（label 值为 `*`），counter 继续前进且坍缩可见。
- 刻意不依赖 `client_golang`：registry 是一片原子量叶子，float 以位的形式存于 `atomic.Uint64`（CAS 循环），label 值严格转义（仅反斜杠、引号、换行），因为 `%q` 会产出 exposition 解析器拒绝的转义。
- `Flush` 等待的是 drain goroutine 归于空闲，而不只是队列读空——一次写失败会在队列已排空后才落入 drop counter，等待必须覆盖这种情况。
- `Metrics` 的每个 recorder 方法都支持 nil receiver，调用方无需自行判空。
- 健康零值可见：两个 drop-counter family（`breakwater_logs_dropped_total`、`breakwater_insights_records_dropped_total`）预创建无 label 的子项，首次 scrape 就显示零。

消费方：[internal/pipeline](../../internal/pipeline/)（observation stage），在 [cmd/breakwater](../../cmd/breakwater/) 组装；`/admin/insights` 背后的监测记录住在 [internal/insights](../../internal/insights/)。
