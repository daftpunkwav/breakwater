# breakwater/

> 语言：**简体中文** | [English](README.md)

网关二进制与其 composition root：唯一决定"每个关注点由哪个 backend 服务、一切以什么顺序接线"的地方。行为都住在 `internal/*` 包里；本目录只跨越进程边界（信号、退出码、root logger）并负责组装。配置只经 `internal/config` 从 `BREAKWATER_*` 环境变量到达——没有任何 CLI flag。

## Files

| 文件 | 职责 |
|---|---|
| `main.go` | 进程边界：root `slog` logger、Makefile 注入的 `version`、`config.Load()`，然后交给 `serve`。刻意保持单薄；`serve` 返回错误而非直接退出，整条路径因此可测 |
| `serve.go` | 组装心脏：装配顺序、每种格式的 middleware 链、运行生命周期（SIGINT/SIGTERM、关闭顺序、drain 超时处理） |
| `governance.go` | backend 选择：`newGovernance`（Redis 需通过 fail-fast Ping，或内存实现）、`newAuthStore`（PostgreSQL / 静态 `BREAKWATER_IDENTITY` / 无，均包在 `auth.NewCachedStore` 里，正缓存 60 s、负缓存 5 s）、`seedBalances` |
| `obsassembly.go` | 观测组装：JSONL access log 文件 sink（`BREAKWATER_ACCESS_LOG_PATH`）、insights store（`BREAKWATER_INSIGHTS_DSN`，默认取 `BREAKWATER_POSTGRES_DSN`）、`combinedSink` 扇出、`buildAdmin` |
| `recovery.go` | 主动恢复循环：探测 auto-disabled 的上游（`BREAKWATER_PROBE_THRESHOLD` 次连续健康应答解除禁用），并为被 breaker 弹出的上游驱动合成探测 |

## Assembly order

`serve` 按此顺序构建：metrics registry → access log + insights store，扇出进一个 `combinedSink` → governance backend（limiter + quota ledger；配置了 Redis 就必须通过 Ping，否则启动失败）→ breaker registry（或 `circuit.NopBreaker{}`）→ identity store（identity 是 governance pipeline 的开关：未配置时，pipeline 在无 governance stage 的状态下运行并记一条警告）→ governance middleware 模板（observation、auth、model authorization、concurrency、rate limit、quota、可选 cache），外加它启动的 sweeper、reconciler 与余额 seeding worker → 上游 bindings 与 router → relay engine → 每种客户端格式一条 `pipeline.Chain`（carrier → request id → format → governance → recovery）→ admin handler → HTTP server → 后台 worker（drop counter 发布、恢复循环）。

## Invariants

- 组装只发生在这里（repo zoning 规则）：`internal/*` 里没有任何接线决策。
- 配置了 Redis 却无法应答，是部署失败而非降级启动——limiter 对它是 fail-close 的。Redis 未设 `BREAKWATER_REDIS_NAMESPACE` 会触发启动警告（多部署共享实例的隐患）。
- access log 在进程退出前排空——happy path、错误路径与 drain 超时一视同仁；server drain 超出其宽限窗口是带警告的正常停止，不是失败，access log 的排空另有一个 5 s 的专属窗口（`shutdownLogGrace`）。
- fallback 链与 context ceiling 引用了不存在的模型名会拒绝启动（`validateModelNames`）；拼错的模型名绝不允许静默过滤。

环境变量参考：根 [README](../../README.md)。本地栈：[deploy/docker-compose.yml](../../deploy/docker-compose.yml)；接入真实 provider：[docs/DEPLOY-LOCAL.md](../../docs/DEPLOY-LOCAL.md)。
