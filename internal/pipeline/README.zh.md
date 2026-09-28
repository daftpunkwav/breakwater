# pipeline/

> 语言：**简体中文** | [English](README.md)

请求侧的 governance pipeline：`net/http` handler 上的有序 middleware 组合，
外加各阶段共享的每请求状态。每种 client format 组装一条 chain；固定顺序在
请求侧由外向内、响应侧由内向外读——因为在 `next()` 之前预留的阶段会在其
返回后结算：

    request side:  carrier -> request id -> format -> observation
                   -> auth -> model authorization -> concurrency
                   -> limiter -> quota(reserve) -> cache -> route -> forward
    response side: forward -> retry/circuit -> quota(settle) -> cache write -> obs

本包只拥有阶段机制本身——limiter、quota、cache 与 relay 各自成包，以
middleware 或末端 handler 的身份接入。

## Files

| File | 职责 |
| --- | --- |
| `chain.go` | `Chain`/`Middleware`：有序组合，先列出者最外层 |
| `carrier.go` | `Carrier`：每请求的类型化结构（身份、已解析请求、token 估算、lease 与消耗句柄），在 chain 入口组装一次；`RequireCarrier`、`SetBody` |
| `body.go` | `FormatStage` 钉住 client format；`EnsureBody` 把有上限的 body 恰好读一次（`protocol.MaxBodyBytes` 上限）并经该 format 的 wire ingest |
| `requestid.go` | `RequestIDStage`：采纳格式良好的客户端 `X-Request-Id`（8–128 个可打印 ASCII）或铸造 `req-` 前缀新 id；回显在每一个响应上，拒绝响应也不例外 |
| `obsmiddleware.go` | `ObservationStage`：in-flight gauge、时长 histogram、按结果计数的 request counter、异步 access log 条目（内含映射为日志 schema 的 relay 尝试轨迹）；第一个看到业务请求最终结果的阶段，被拒请求也不例外 |
| `authstage.go` | `AuthStage`：Bearer / `x-api-key` → 经 `auth.Store` 解析 tenant；401/503 以 client format 渲染——store 故障表现为 503，绝不静默放行 |
| `authzstage.go` | `ModelAuthzStage` 与 `AuthorizeModel`：tier 模型授权，置于 auth 之后、任何 governance 花费与 cache 之前；也是 inference handler 调用的同一个权威函数，两处裁决不会漂移 |
| `estimate.go` | `EstimateTokens` / `EstimatePartialTokens` / `PromptTokens`：TPM 桶与 quota lease 共同预留的那一份 pre-call 估算；声明的 `max_tokens` 会被钳制到 tenant 的 per-request 上限（防自我 DoS） |
| `recovery.go` | `RecoveryStage`：响应尚未开始时，handler panic 变成一次渲染的 500；位于最内层，紧贴 endpoint handler |

## Tests

每个阶段一个测试文件（组合、body ingest、failure taxonomy、request-id 采
纳）；`teststore_test.go` 提供共享的假 auth store。

## Invariants

- Retry、circuit breaking 与 failover 绝不能组合成 Middleware：它们包在末端
  forward stage 内部对上游调用的外圈（重放不可能重放一个消费过半的
  `http.Handler` 响应）。它们属于 [`internal/relay`](../relay/)。
- 各阶段包装 `http.Handler`，让 `http.Flusher` 实现穿过每一层存活——这是
  SSE passthrough 的前提。
- body 每请求恰好读一次、存入 carrier；传输层拒绝（过大、不可读、畸形）是
  请求事实，以 client format 的信封渲染，不是阶段的主观意见。
- carrier 是单请求 goroutine 上的哑类型化结构——无锁、不做 context 垃圾袋；
  每个字段由唯一拥有它的阶段写入。
- 模型授权规则只存在一份（`AuthorizeModel`）；pipeline 阶段与 inference
  handler 刻意重复的是执行点，绝不是规则本身。
