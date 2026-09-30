# 本地部署：真实 provider、真实客户端

> 语言：**简体中文** | [English](DEPLOY-LOCAL.md)

这是从全新 checkout 到"用自己的 provider 服务你的 agent 应用"的网关的
操作路径。mock upstream 仍可用于故障注入（见 README），但本篇没有任何
步骤依赖它。

## 1. 部署形态

    agent apps                breakwater (:8080)              providers
    ──────────                ──────────────────              ─────────
    zcode / CLI / IDE  ──▶   /v1/chat/completions   ──▶      provider A
    any OpenAI client  ──▶   /v1/responses          ──▶      provider B
    any Anthropic client ─▶  /v1/messages           ──▶      provider C
                             /admin/*  (ops)
                             /metrics /healthz /readyz /version

客户端对一个本地端口说自己的原生格式；breakwater 鉴权、限流、计量
quota、缓存，然后把每个请求跨已配置的 upstream 路由并 failover。客户端
永远看不到 provider。

## 2. 选择 backend

- **In-memory（零配置）**——无需启动任何东西；状态是进程内的。单人工作
  站足够。
- **Redis**（`BREAKWATER_REDIS_ADDR`）——limiter 桶与 quota 台账迁移到原子
  Lua 脚本；余额跨重启存活。网关在组装期固定客户端自身的操作超时
  （拨号 1s、读写 500ms）：黑洞化的 Redis 会在每个治理阶段一秒内
  fail-closed 拒绝请求，而不是按库默认值把请求拖住数秒。
- **PostgreSQL**（`BREAKWATER_POSTGRES_DSN`）——API key 从 system of record
  （`deploy/schema.sql`）解析，替代内联 identity JSON，且 quota 对账协议
  可以武装。

本地单 tenant 场景，memory 模式加内联 identity 即已完备；需要余额持久化
时再加 Redis。

## 3. 接入你的 provider

Provider 声明在 `BREAKWATER_UPSTREAMS` 中（JSON 列表；列表顺序即每模型的
failover 优先级）。每个 OpenAI-compatible provider——OpenAI、DeepSeek、
Moonshot、OpenRouter、本地 vLLM——用同一个形态。为每个 provider 设置
`api_key`，想让网关呈现统一模型命名空间时，用 `client=real` 条目做别名：

    export BREAKWATER_UPSTREAMS='[
      {"id":"deepseek","base_url":"https://api.deepseek.com",
       "api_key":"sk-...",
       "models":["deepseek-chat","deepseek-reasoner"]},
      {"id":"openai","base_url":"https://api.openai.com",
       "api_key":"sk-...",
       "models":["gpt-4o","gpt-4o-mini",
                 "claude-sonnet=claude-sonnet-4-20250514"]},
      {"id":"fallback-pool","base_url":"https://openrouter.ai/api",
       "api_key":"sk-or-...",
       "models":["*"]}
    ]'

读这个例子：请求 `deepseek-chat` 的客户端去 DeepSeek；`gpt-4o` 去 OpenAI；
`claude-sonnet` 去 OpenAI 的别名长名，若该次尝试失败则转 OpenRouter 的
通配池——一个请求、两家 provider，各自转发它真正服务的模型名。通配
（`*`）绑定无需重写。

Key 留在你的环境变量或 secret store 中；不落盘、不进日志。Probe URL 可选
——没有它，upstream 无法被请求路径之外的任何东西询问，恢复只能依赖真实
流量。配置了它（且 `BREAKWATER_PROBE_INTERVAL` 非零，默认如此）时，恢复
loop 周期性探测 auto-disabled 与被 breaker 逐出的 upstream，健康应答后
恢复它们；想让凭据或 quota 故障自愈，就把它指向一个需要鉴权的端点。致命
损坏的 upstream——凭据被拒或 quota 耗尽——也会自行退出轮换，原因在
`GET /admin/routing` 可见。

## 4. 把 agent 应用指向网关

任何接受自定义 OpenAI-compatible base URL 的客户端无需改动即可工作。给它
网关地址和一个 tenant API key（下面的 identity 接续 README quick start）：

    BREAKWATER_IDENTITY='{"tiers":[
        {"id":"free","rpm":600,"tpm":900000,"max_tokens":4096,
         "monthly_quota":50000000,"allowed_models":["*"]}],
      "tenants":[{"id":"local","name":"Local","tier":"free",
         "keys":["bw-local-dev-key"]}]}' \
    BREAKWATER_ADMIN_TOKEN='dev-admin' \
    go run ./cmd/breakwater

然后配置 agent（变量名以其文档为准）：

    OPENAI_BASE_URL=http://127.0.0.1:8080/v1
    OPENAI_API_KEY=bw-local-dev-key

Anthropic 原生客户端指向 `http://127.0.0.1:8080`，对 `/v1/messages` 说话，
带 `x-api-key: bw-local-dev-key`。三种 client format 共享一条 pipeline，
所以 agent 想按请求切换模型时只需换个模型名——网关对收到的任何东西做
路由、计量与 failover。

## 5. 或者跑组合 stack

    docker compose -f deploy/docker-compose.yml up -d --build

会拉起 Redis、PostgreSQL（首次启动应用 schema + seed）、mock upstream 与
网关。要用真实 provider，在 `up` 之前把你上面拼好的 `BREAKWATER_UPSTREAMS`
导出到 compose 环境中 `breakwater` service 的环境（或扩展
`deploy/docker-compose.yml` 的 `environment` 块）。

组合网关从 `BREAKWATER_IDENTITY` 解析 key，不设置
`BREAKWATER_POSTGRES_DSN`，因此不读已 seed 数据库的任何行——那些行是为
load-test 场景准备的，它们用 seed key 调用网关。加入 DSN 即可让数据库成为
网关的 system of record，届时 seed tenant 成为线上 tenant。

## 6. 运维

| 需求                                | 操作面                                             |
| ----------------------------------- | -------------------------------------------------- |
| 某 provider 现在坏了或在拖吗？      | `GET /admin/breakers` 与 `GET /metrics`（熔断策略决定什么算数：连续故障、上升的错误/拒绝占比，或慢完成） |
| 立即摘掉一个行为不端的模型          | `PUT /admin/models/{id}` `{"enabled": false}`      |
| 排空某 provider（维护）             | `PUT /admin/upstreams/{id}` `{"enabled": false}`   |
| 现在关了哪些开关？                  | `GET /admin/routing`                               |
| 接入一个用户（管理员或员工）        | `POST /admin/users` `{"name","tier","role"}`       |
| 签发一个 key（每用户至多 5 个）     | `POST /admin/users/{id}/keys` `{"name"}`           |
| 对某用户禁用一个模型                | `PUT /admin/users/{id}/limits` `{"denied_models":[...]}` |
| 收紧单个 key（quota/rpm/concurrency）| `PUT /admin/keys/{id}/limits` `{...}` |
| 吊销一个泄露的 key                  | `PUT /admin/keys/{id}/status` `{"enabled": false}` |
| Tenant 余额 / 充值                  | `GET`/`PUT /admin/tenants/{id}/quota`              |
| 稳定性报告（评估视图）              | `GET /admin/insights?hours=24`——成功率、失败原因、延迟分位数、按 tenant/key/model/upstream 切片 |
| 每请求审计轨迹                      | `BREAKWATER_ACCESS_LOG_PATH` JSONL；`RequestID` 成员即 `X-Request-Id` 的值 |

监控存储（`BREAKWATER_INSIGHTS_DSN`，默认取 identity DSN）为每个完成的
请求持久化一行——包括失败原因：governance 拒绝带自己的 code
（`invalid_api_key`、`rate_limit_exceeded`、`insufficient_quota`、
`concurrency_limit_exceeded`），网关失败带自己的（`no_upstream`、
`circuit_open`、`budget_exhausted`、`upstream_unreachable`），上游错误
passthrough 按状态类分类。客户端断连绝不计为失败。

用户与 key 管理需要 PostgreSQL identity store；limits 层按 tier → user →
key 合并（标量就近取胜、deny 取并集、allow 只收紧），在 auth cache TTL 内
生效。部署守卫也会拒绝启动"武装但无 token"的配置（upstreams / identity /
insights 任一存在却无 `BREAKWATER_ADMIN_TOKEN`）。只要端口在你的机器之外
可达，就用 `BREAKWATER_ADMIN_TOKEN` 守住 admin
面。Routing 开关在内存中、重启即重置；永久移除是配置变更。

设置 `BREAKWATER_ROUTING_STRATEGY=latency`，同模型候选即按实测交换延迟排序
而非配置顺序——tracker 为每次 attempt 打分（客户端取消除外），未试过的
upstream 优先探索。两种模式下 breaker 与运维开关都把守 eligibility；策略
只管排序。

## 7. 验证

    # 一次缓冲调用，任意格式
    curl -s http://127.0.0.1:8080/v1/chat/completions \
      -H 'Authorization: Bearer bw-local-dev-key' \
      -H 'Content-Type: application/json' \
      -d '{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}'

    # 同一段对话，Anthropic 格式
    curl -s http://127.0.0.1:8080/v1/messages \
      -H 'x-api-key: bw-local-dev-key' \
      -H 'anthropic-version: 2023-06-01' \
      -H 'Content-Type: application/json' \
      -d '{"model":"claude-sonnet","max_tokens":64,
           "messages":[{"role":"user","content":"hi"}]}'

    # 流式
    curl -sN http://127.0.0.1:8080/v1/chat/completions \
      -H 'Authorization: Bearer bw-local-dev-key' \
      -H 'Content-Type: application/json' \
      -d '{"model":"deepseek-chat","stream":true,
           "messages":[{"role":"user","content":"hi"}]}'

把响应中的 `X-Request-Id` 与 access log 行、provider 自己的请求日志对照；
同一个 id 应出现在全部三处。
