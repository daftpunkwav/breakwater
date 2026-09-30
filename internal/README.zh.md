# internal/

> 语言：**简体中文** | [English](README.md)

网关的全部能力包。约束所有包的 zoning 规则（根 README "Layout zoning"）：

1. Governance 机制自包含——实现、Lua 脚本、sweeper 与各自的 pipeline
   middleware 都在包内生长，绝不拆子包。
2. Provider adapter 保持扁平：每个 OpenAI-compatible provider 都由
   `internal/upstream/openai.go` 以不同 `base_url` 配置服务。
3. 一切 HTTP endpoint 形态的东西属于 `internal/server`；一切 wire 格式形态
   的东西属于 `internal/protocol`。`internal/httpserver` 只放中立的、纯
   stdlib 的传输机制。
4. 组合只发生在 `cmd/*` 根中。

| 包 | 职责 | README |
| --- | --- | --- |
| [`auth/`](auth/) | 身份：users、roles、分层 key limits（static/PostgreSQL store、进程内 LRU） | [链接](auth/README.md) |
| [`cache/`](cache/) | 精确匹配缓存、手写 singleflight、eligibility | [链接](cache/README.md) |
| [`circuit/`](circuit/) | 同一 port 后的 breaker：三态连续失败状态机，或窗口化 ratio 守卫（含无 breaker 运行用的 nop） | [链接](circuit/README.md) |
| [`config/`](config/) | 配置 schema 与加载（`BREAKWATER_*` env → 类型化配置） | — |
| [`httpserver/`](httpserver/) | 共享 HTTP 生命周期与响应 tee | — |
| [`insights/`](insights/) | 监控记录存储：批量异步写入、稳定性聚合（成功率、失败构成、分位数、时间线） | — |
| [`limiter/`](limiter/) | RPM/TPM token 桶（in-memory + Redis Lua）、每 tenant 并发闸、429 阶段 | [链接](limiter/README.md) |
| [`mockllm/`](mockllm/) | 进程内 mock upstream 库，带 fault injection；`cmd/mockllm` 是其 CLI 包装 | [链接](mockllm/README.md) |
| [`obs/`](obs/) | 有界异步 access log、手写 metrics registry | [链接](obs/README.md) |
| [`pipeline/`](pipeline/) | Middleware 链、每请求 carrier、模型授权、observation 阶段 | [链接](pipeline/README.md) |
| [`protocol/`](protocol/) | Wire 契约：canonical chat 形态、translator wire、SSE codec | [链接](protocol/README.md) |
| [`quota/`](quota/) | Lease 台账（in-memory + Redis Lua）、sweeper、402 阶段 | [链接](quota/README.md) |
| [`relay/`](relay/) | 响应侧执行引擎：attempt、failover、SSE passthrough、诚实流终结 | [链接](relay/README.md) |
| [`retry/`](retry/) | Attempt loop、预算、retryability 分类器 | [链接](retry/README.md) |
| [`router/`](router/) | 候选选择：static 优先级或实测延迟排序、breaker 预过滤、运行时运维开关 | [链接](router/README.md) |
| [`server/`](server/) | Route 装配、三个 inference endpoint、admin API（operations + identity administration） | [链接](server/README.md) |
| [`upstream/`](upstream/) | Provider port + OpenAI-compatible adapter | — |

没有 README 的包是小叶子；本表中的那一行就是它们的文档。inference chain 的
顺序——auth → model authorization → concurrency → limiter → quota →
cache → route → forward——依次穿过 `pipeline`、`auth`、`limiter`、`quota`、
`cache`、`router` 与 `relay`，最后经 `protocol` 渲染。
