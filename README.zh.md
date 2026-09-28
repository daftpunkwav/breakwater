# Breakwater

> 语言：**简体中文** | [English](README.md)

一个用 Go 编写的 LLM 网关，以同一条 canonical governance pipeline 服务三种
客户端 API 格式——**OpenAI Chat Completions**（`/v1/chat/completions`）、
**OpenAI Responses**（`/v1/responses`）与 **Anthropic Messages**
（`/v1/messages`）——为证明一个论题而构建：高并发可靠性治理——限流、带
failover 的熔断、有界重试、缓存击穿防护、基于 lease 的 quota 一致性与异步
可观测性——全部手工实现，并以可复现的测试、指标与故障注入实验背书。

## 组件

| 路径                  | 职责                                                      |
| --------------------- | --------------------------------------------------------- |
| `cmd/breakwater`      | 网关二进制（组合根）                                       |
| `cmd/mockllm`         | Mock OpenAI-compatible upstream，带故障注入                 |
| `internal/server`     | Route 装配、三个 inference endpoint、admin API（operations + identity administration） |
| `internal/relay`      | 响应侧执行引擎：attempt、failover、SSE passthrough、诚实流终结 |
| `internal/pipeline`   | Middleware 链、每请求 carrier、模型授权、observation 阶段   |
| `internal/httpserver` | 共享 HTTP 生命周期与响应 tee                                |
| `internal/insights`   | 监控记录存储：批量异步写入、稳定性聚合（成功率、失败构成、分位数、时间线） |
| `internal/protocol`   | Wire 契约：canonical chat 形态、translator wire（openai-chat passthrough、openai-responses 与 anthropic-messages 转码）、SSE codec |
| `internal/auth`       | 身份：users、roles、分层 key limits（static/PostgreSQL store、进程内 LRU） |
| `internal/limiter`    | RPM/TPM token 桶（in-memory + Redis Lua）、每 tenant 并发闸、429 阶段 |
| `internal/quota`      | Lease 台账（in-memory + Redis Lua）、sweeper、402 阶段      |
| `internal/cache`      | 精确匹配缓存、手写 singleflight、eligibility               |
| `internal/circuit`    | 三态 breaker（含无 breaker 运行用的 nop）                  |
| `internal/retry`      | Attempt loop、预算、retryability 分类器                    |
| `internal/router`     | 候选选择：static 优先级或实测延迟排序、breaker 预过滤、运行时运维开关 |
| `internal/upstream`   | Provider port + OpenAI-compatible adapter                  |
| `internal/config`     | 配置 schema 与加载                                          |
| `internal/obs`        | 有界异步 access log（含逐 attempt 轨迹）、手写 metrics registry |
| `deploy`              | docker-compose stack、schema、seed、容器构建                |
| `loadtest`            | k6 场景，每个瞄准系统的一条性质                             |
| `docs`                | 基准、故障注入报告、本地部署指南与测试约定                   |

## 布局分区

目录树是封闭的：未来的增长以新文件落入既有包内的形式发生，绝不新增顶层
目录。分区规则：

1. Governance 机制自包含：实现、Lua 脚本、sweeper 与各自的 pipeline
   middleware 在自己的包内生长——绝不拆子包。
2. Provider adapter 保持扁平：每个 OpenAI-compatible provider 都由
   `internal/upstream/openai.go` 以不同 `base_url` 配置服务。该包每个
   provider 关注点一个文件（`upstream.go`、`openai.go`、`modelmap.go`、
   `transport.go`），而不是 adapter 树。
3. 一切 HTTP endpoint 形态的东西属于 `internal/server`（业务 `/v1/*`、
   `/admin/*`、探针）；一切 wire 格式形态的东西属于 `internal/protocol`。
4. `internal/httpserver` 只放中立的、纯 stdlib 的传输机制；组合只发生在
   `cmd/*` 根中。

## 客户端格式

| 路由                       | 格式                  | 鉴权 header              | 说明 |
| -------------------------- | --------------------- | ------------------------ | ----- |
| `POST /v1/chat/completions`| OpenAI Chat（canonical）| `Authorization: Bearer` | 到 OpenAI-compatible upstream 的字节 passthrough；SSE passthrough |
| `POST /v1/responses`       | OpenAI Responses      | `Authorization: Bearer`  | 转码：input/instructions/max_output_tokens 进，Responses 对象与 `response.*` 事件出 |
| `POST /v1/messages`        | Anthropic Messages    | `x-api-key` 或 Bearer    | 转码：system/blocks/必需 `max_tokens` 进，Messages 对象与 `message_*` 事件出；`stop_sequences` 被拒绝而非静默丢弃 |

每个 inference 响应携带 `X-Request-Id` header：格式良好的客户端提供的 id 被
原样采纳，否则铸造一个（`req-` 前缀）。该 id 一路传到 upstream 交换与
access log，让一个标识符串起客户端可见的结果、网关的日志行与 provider 的
记录。关联阶段属于 inference 链：admin、discovery 与探针路由不携带它。

`GET /v1/models` 以 OpenAI list 形式列出 client-facing 模型名，让
OpenAI-compatible 客户端能发现可请求什么。该端点不鉴权，不含 tenant 数据。

### 模型名与别名

网关按客户端发送的模型名路由。`models` 条目形如 `"client=real"` 时，以转发
provider 真实名的方式服务 client-facing 名——adapter 重写请求体的 `model`
字段，其余字节原样通过：

    "models": ["claude-sonnet=claude-sonnet-4-20250514", "deepseek-chat"]

同一个 client-facing 名绑定到多个 upstream 时，一个请求自带 failover 链
——且因为每个 upstream 重写为自己的真实名，请求中途的 failover 可以跨
provider、跨模型，而不只是跨主机。

链可以跨模型延伸：`BREAKWATER_FALLBACKS` 把一个模型映射到有序的 fallback
模型，在它自己的候选耗尽后按序尝试——每一跳重新解析候选、重写转发体的
模型名，且绝不重新进入已试过的模型。链是偏好列表而非契约：无法服务的跳
（被禁用、未绑定、prompt 超出其 context 上限）会被跳过。

三者走完全相同的 governance pipeline（auth → 模型授权 → concurrency →
rate limit → quota → cache）与完全相同的 relay 引擎；只有 wire 不同。
canonical wire 是 openai-chat：未知请求字段以字节 passthrough 存活，而
translated 格式摄取一个已声明的子集，拒绝它们无法诚实表达的部分。
Translated 格式只收文本：非文本的 input part 或 message block 在摄取时被
拒。canonical wire 原样转发，所以 tool 声明与 tool 消息原样通过，而多模态
的 `content` 数组无法通过 canonical 解码，作为畸形请求被拒。精确匹配缓存
只服务 canonical wire（translated 格式的重放需要响应重渲染，这是刻意不做
的伪装）。

## Quick start

以 in-memory governance backend 启动 mock upstream 与网关（冒烟运行无需
Redis）：

    go run ./cmd/mockllm -addr 127.0.0.1:8090

    BREAKWATER_UPSTREAMS='[{"id":"mock","base_url":"http://127.0.0.1:8090","probe_url":"http://127.0.0.1:8090/healthz","models":["*"]}]' \
    BREAKWATER_IDENTITY='{"tiers":[{"id":"free","rpm":60,"tpm":200000,"max_tokens":4096,"monthly_quota":10000000,"allowed_models":["*"]}],"tenants":[{"id":"local","name":"Local","tier":"free","keys":["bw-local-dev-key"]}]}' \
    go run ./cmd/breakwater

经网关发送一次补全：

    curl -s http://127.0.0.1:8080/v1/chat/completions \
      -H 'Authorization: Bearer bw-local-dev-key' \
      -H 'Content-Type: application/json' \
      -d '{"model":"mock-gpt","messages":[{"role":"user","content":"hi"}]}'

流式发送一次：

    curl -s -N http://127.0.0.1:8080/v1/chat/completions \
      -H 'Authorization: Bearer bw-local-dev-key' \
      -H 'Content-Type: application/json' \
      -d '{"model":"mock-gpt","stream":true,"messages":[{"role":"user","content":"hi"}]}'

配置 Redis（`BREAKWATER_REDIS_ADDR`）后，limiter 与 quota 台账运行在原子
Lua 脚本上，identity tier 余额自动从静态 identity 集合 seed。配置
`BREAKWATER_POSTGRES_DSN` 后，API key 从 PostgreSQL 解析（schema 见
`deploy/schema.sql`，本地 seed 见 `deploy/seed.sql`），否则
`BREAKWATER_IDENTITY` JSON 就是 system of record。

## 配置

全部配置走环境变量；核心旋钮：

| 变量                                  | 默认           | 作用                                                       |
| ------------------------------------- | -------------- | ---------------------------------------------------------- |
| `BREAKWATER_ADDR`                     | `:8080`        | 监听地址                                                    |
| `BREAKWATER_UPSTREAMS`                | _(无)_         | upstream 的 JSON 列表（`id`、`base_url`、`probe_url`、`api_key`、`api_keys`、`models`；列表顺序 = failover 优先级；`"client=real"` 条目做模型别名；`api_keys` 在同一 upstream 后轮换多把凭据） |
| `BREAKWATER_ROUTING_STRATEGY`         | `static`       | 候选顺序：`static`（配置顺序）或 `latency`（实测交换延迟优先；近乎打平的候选按请求轮换领先权，其余并列由配置顺序裁决；未试过的 upstream 优先探索） |
| `BREAKWATER_FALLBACKS`                | _(无)_         | 模型 → 有序 fallback 模型的 JSON 映射，在主模型每个候选耗尽后尝试（`{"gpt-4o":["gpt-4o-mini"]}`）；键与目标必须指向已配置的 client-facing 模型 |
| `BREAKWATER_CONTEXT_LIMITS`           | _(无)_         | 模型 → 最大输入 token 估算的 JSON 映射；超出上限的 prompt 提前拒绝该模型的全部候选，返回 `413 context_window_exceeded`，而不是注定失败的 upstream 交换 |
| `BREAKWATER_IDENTITY`                 | _(无)_         | JSON identity 集合（`tiers`、带 `role` 与用户级 `overrides` 的 `tenants`）；武装 governance pipeline |
| `BREAKWATER_POSTGRES_DSN`             | _(无)_         | 身份 system of record（覆盖静态集合）                        |
| `BREAKWATER_REDIS_ADDR`               | _(无)_         | 启用 Redis backend；不设则 in-memory                         |
| `BREAKWATER_REDIS_NAMESPACE`          | _(无)_         | 给每个 limiter 与 quota key 加前缀。两个部署共享一个 Redis 实例时必须设置——否则它们共享 tenant 余额，一个环境的 sweeper 会退另一个环境的在用 lease。留空表示本网关独占其 Redis。**在已有余额的部署上设置它会让所有既有 key 被弃用（不做任何迁移），静态 identity seeder 随后按全月预算重新给每个 tenant 入账**——把它当作重置而非改名。PostgreSQL-identity 部署没有 seeder，那里改 namespace 会让所有 tenant 没有台账，直到有人入账。 |
| `BREAKWATER_QUOTA_LEASE_TTL`          | _派生_         | 预留的 quota lease 在 sweeper 回收退款前可存活的时长。由 `OverallDeadline + StreamTimeout` 加余量派生，因为短于最长请求的视野会静默退还一个真实花了 token 的请求；低于该值的配置在启动时被拒。无界流或 deadline 没有可派生的东西，所以那些情况默认 24h。 |
| `BREAKWATER_RETRY_MAX_ATTEMPTS`       | `3`            | 每请求的 upstream attempt 数                                 |
| `BREAKWATER_RETRY_BUDGET_MAX_IN_FLIGHT` | `64`         | 进程级并发重试上限；配置了份额预算时被取代                    |
| `BREAKWATER_RETRY_BUDGET_PERCENT`     | _(关)_         | 份额预算：并发重试至多占当前在途请求的这个百分比——上限随实时流量伸缩，而不是一个静态数字。`0` 保持固定上限。 |
| `BREAKWATER_RETRY_BUDGET_MIN_IN_FLIGHT` | `3`          | 份额预算的下限：无论网关多空闲，重试上限不低于它              |
| `BREAKWATER_STREAM_TIMEOUT`           | `10m`          | 已提交流的响应体（header 之后）的天花板；`0` 让客户端拥有流的生命周期。保持比 attempt timeout（约束 time-to-first-byte）更宽松：否则慢 header 会被这个天花板切断，请求转而 failover 而不是等待。 |
| `BREAKWATER_CACHE_ENABLED` / `_TTL` / `_CAPACITY` | on / `60s` / `1024` | 精确匹配响应缓存           |
| `BREAKWATER_CIRCUIT_*`                | on / `5` / `30s` / `5s` | breaker 阈值、cooldown、probe 超时        |
| `BREAKWATER_PROBE_INTERVAL` / `_TIMEOUT` / `_THRESHOLD` | `30s` / `5s` / `2` | 退出轮换的 upstream 的主动恢复探测；interval `0` 禁用（恢复只能等真实流量）；auto-disable 的 upstream 需要 `threshold` 次连续健康探测才恢复 |
| `BREAKWATER_ACCESS_LOG_PATH`          | _(关)_         | JSONL access log 文件（有界队列、丢最旧）                    |
| `BREAKWATER_ACCESS_LOG_QUEUE_SIZE`    | `4096`         | access log 内存队列容量；溢出丢弃条目并计数                  |
| `BREAKWATER_ADMIN_TOKEN`              | _(无)_         | 守护 `/admin/*` 的 Bearer token（留空 = 不设防，仅限开发）   |
| `BREAKWATER_RECONCILE_INTERVAL`       | `1m`           | Quota 台账对账节拍；需要 Redis + PostgreSQL；`0` 禁用        |
| `BREAKWATER_INSIGHTS_DSN`             | _(主 DSN)_     | 监控记录持久化到的 PostgreSQL；默认取 `BREAKWATER_POSTGRES_DSN`；无任何 DSN 时未设 |

## 运维面

- `GET /healthz` — liveness
- `GET /readyz` — readiness（以 fail-closed limiter 依赖的 backend 为闸；
  它不说谎）
- `GET /metrics` — Prometheus 文本暴露（手写 registry）
- `GET /version` — 构建标识与 Go 版本
- `GET /admin/tenants/{id}/quota` — 当前余额
- `PUT /admin/tenants/{id}/quota` — 充值或修正余额（`{"balance": N}`）；对账
  协议把修正间隔视为设计内跳过
- `GET /admin/breakers` — 每 upstream 的 breaker 状态
- `POST /admin/breakers/{id}/reset` — 强制打开的 breaker 关闭
- `POST /admin/upstreams/{id}/probe` — 按需做一次健康探测
- `GET /admin/insights?hours=N` — 尾随窗口（默认 24）的稳定性报告：成功率、
  按原因的失败构成、延迟分位数、5 分钟时间线与按 tenant/key/model/upstream
  的拆分。需要监控存储（任意 PostgreSQL DSN）。
- `GET /admin/routing` — 每个已知模型与 upstream 的当前 eligibility，外加
  auto-disable 的 upstream 及每次决策的原因与时刻
- `PUT /admin/models/{id}` — 启用或禁用模型（`{"enabled": false}`）；被禁模型
  以 `403 model_disabled` 拒绝请求
- `PUT /admin/upstreams/{id}` — 启用或禁用 upstream；被禁 upstream 从每个候选
  列表消失。开关在内存中，重启即重置。运维启用会同时清除两个禁用通道
  （见下）。

### Upstream 自动恢复

在运维开关之外，网关自己也盯紧 upstream：

- 一次完成的 upstream 交换证明**致命条件**——凭据被拒（401，或 403 且其
  provider 错误信封属凭据类——type `authentication_error` 或 code
  `invalid_api_key`；模型访问、地区或内容策略类的 403 保持可重试）或预算
  耗尽（`insufficient_quota`，OpenAI 以 429 报告）——立即把它退出轮换，
  原因记录在 `/admin/routing`，并计入
  `breakwater_upstream_auto_disabled_total`。
- 携带 `Retry-After` header 的 upstream 429 只推迟**该 upstream 自己**的下
  一次重试；向其他候选的 failover 绝不等失败者的提示。
- 恢复 loop（`BREAKWATER_PROBE_INTERVAL` 非零且 upstream 声明了
  `probe_url` 时）周期性探测 auto-disable 与被 breaker 逐出的 upstream；
  恢复一个 auto-disable 的 upstream 需要 `BREAKWATER_PROBE_THRESHOLD` 次
  连续健康探测（一次失败清零计数），flapping 的 upstream 无法循环回归。
  只有系统自己的禁用可以这样解除——运维禁用存活到运维亲自解除。想让凭据
  与 quota 故障自愈，就把 `probe_url` 指向一个需要鉴权的端点。
- 两个运维把手补完这个环：`POST /admin/breakers/{id}/reset` 强制打开的
  breaker 关闭（"我修好了 upstream，现在放行"），`POST
  /admin/upstreams/{id}/probe` 按需跑一次健康交换（200 健康，upstream 未
  声明 `probe_url` 时 409，探测失败 502）。

### 凭据环

一个 upstream 可以同时持有多把 provider 凭据——`api_keys` 列出在同一
`base_url` 后轮换的其余 bearer token（设置了的 `api_key` 领衔）。供应商的
限流按凭据计，环因此把 upstream 的流量摊到所有凭据上：每次 exchange 取下一
把存活凭据，并发请求落在不同凭据上，而本请求中已被限流或被判定 fatal 的
凭据在该请求内不再复用。

fatal 条件（凭据被拒、预算耗尽）定罪的是凭据，不是 upstream：环退役该凭据
——计入 `breakwater_credential_retired_total`——其余凭据继续服务。只有最后
一把存活凭据死亡，才经与此前相同的 auto-disable 把 upstream 移出轮转；而
解除该禁用的任何路径（操作员启用、恢复探测转健康）都会把环恢复满编。被
退役的凭据本身只随 upstream 的重新进入而复位——没有按凭据的探测。

attempt 预算仍然约束整个游走：一个请求至多发出
`BREAKWATER_RETRY_MAX_ATTEMPTS` 次 exchange，想让 fatal 游走走完整个环，
就把该值调到不小于环的大小。

### 身份管理（PostgreSQL 部署）

配置 `BREAKWATER_POSTGRES_DSN` 后，身份是带 key 的用户——每用户至多五把
——且每一层携带自己的限额层：

- `POST /admin/users` — 创建用户（`{"name", "tier", "role"}`；role 为
  `user` 或 `admin`）
- `GET /admin/users` — 列出用户
- `PUT /admin/users/{id}/limits` — 用户级限额层，作用于该用户的每把 key
- `POST /admin/users/{id}/keys` — 签发 key（原始密钥只显示一次）
- `GET /admin/users/{id}/keys` — 列出用户的 key
- `PUT /admin/keys/{id}/limits` — key 级限额层
- `PUT /admin/keys/{id}/status` — 禁用或重新启用一把 key（`{"enabled": false}`）

限额层（下面的 `LIMITS`）是一份 JSON 文档：

```json
{"rpm": 30, "tpm": 50000, "max_tokens": 2048, "monthly_quota": 1000000,
 "concurrency": 4,
 "allowed_models": ["m1", "m2"],
 "denied_models": ["expensive-model"],
 "model_quotas": {"expensive-model": 100000}}
```

解析把 tier 模板、用户层与 key 层合并：标量取最近设置的值（key 覆盖
user 覆盖 tier）；`denied_models` 跨层取并集——用户级 deny 把该模型从每把
key 上移除；`allowed_models` 只收紧（`*` 通配的 tier 加上具名的用户列表，
意味着恰好那个列表）；`model_quotas` 按模型合并。省略的字段继承；`{}`
清空该层。变更在 auth cache TTL 内作用于线上流量。静态
`BREAKWATER_IDENTITY` 模式支持 `role` 与 tenant 级 `overrides` 文档，但
没有管理面。

## 故障注入接口

经 header 的每请求指令：

| Header                        | 作用                                                       |
| ----------------------------- | ---------------------------------------------------------- |
| `X-Mockllm-Delay-Ms`          | 首个响应字节前的延迟                                        |
| `X-Mockllm-Status`            | 以该错误状态应答（400..599）                                |
| `X-Mockllm-Stream-Mode`       | `normal`（默认）/ `slow` / `abort`                          |
| `X-Mockllm-Chunk-Delay-Ms`    | `slow` 模式的 chunk 间延迟                                  |
| `X-Mockllm-Omit-Usage`        | 从响应中删掉 usage 字段                                     |
| `X-Mockllm-Completion-Tokens` | 固定的 completion 长度（词数）                              |

进程级 chaos 旋钮：`-default-delay`、`-error-rate`。

示例：一条中途死掉的流——网关保留已交付的 chunk，以一帧流内错误事件加
`[DONE]` 诚实终结：

    curl -s -N http://127.0.0.1:8090/v1/chat/completions \
      -H 'X-Mockllm-Stream-Mode: abort' \
      -H 'Content-Type: application/json' \
      -d '{"model":"mock-gpt","stream":true,"messages":[{"role":"user","content":"hi"}]}'

## 开发者任务

Makefile 包装常用循环（`make test`、`make race`、`make cover`、
`make lint`、`make build`、`make up`）。`make build` 把 git 派生的版本注入
二进制。

## 本地 stack

    docker compose -f deploy/docker-compose.yml up -d

拉起 Redis、PostgreSQL（首次启动应用 identity schema 与本地 seed）、mock
upstream 与网关本身。给组合网关发一个请求：

    curl -s http://127.0.0.1:8080/v1/chat/completions \
      -H 'Authorization: Bearer bw-local-dev-key' \
      -H 'Content-Type: application/json' \
      -d '{"model":"mock-gpt","messages":[{"role":"user","content":"hi"}]}'

## 质量门

CI 强制 `gofmt`、`go vet`、`golangci-lint` 与 `go test -race ./...`（带
Redis 与 PostgreSQL service container，供 Lua 与 identity 集成测试）。
该环境下每包满足 95% 语句覆盖门槛；本地无 Redis 与 PostgreSQL 的运行读数
更低，因为三个包的 SQL 路径在数据库门控测试之后，`docs/TESTING.md` 列出
两种环境的实测数字。分类学（unit / functional / integration / concurrency
/ benchmark）与命名规则见 `docs/TESTING.md`。可靠性机制（token 桶、
circuit breaker、singleflight、限流）以纪律在本仓库手工实现；对应的四个
第三方库被 lint 规则 `no-off-the-shelf-governance` 在 lint 时拒绝。

## 文档

证据方法论与场景集在 `docs/`：`docs/BENCHMARK.md`（负载测试数字）、
`docs/CHAOS-REPORT.md`（故障注入时间线）与 `docs/DEPLOY-LOCAL.md`（把真实
provider 与 agent 应用接入本地网关）。BENCHMARK 与 CHAOS-REPORT 由
`loadtest/` 场景的真实运行填入——见 `loadtest/README.md`。

## 许可证

MIT — 见 [LICENSE](LICENSE)。
