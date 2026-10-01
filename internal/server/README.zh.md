# server/

> 语言：**简体中文** | [English](README.md)

一切 HTTP endpoint 形态的东西：route 装配、三个 inference endpoint
（openai-chat、openai-responses、anthropic-messages）、admin API 的两半
（operations + identity administration）、探针与发现端点。listen/serve/drain
生命周期委托给 [`internal/httpserver`](../httpserver/)，让每个二进制共享同
一套关闭顺序。wire 格式的事属于 [`internal/protocol`](../protocol/)，不属于
这里。

## Files

| File | 职责 |
| --- | --- |
| `routes.go` | `newRootHandler`：把显式的 URL 布局集中在一处；整个 mux 外包一层 recovery，使非 inference 路由也能渲染 500 而不是被掐断连接 |
| `server.go` | `Server`/`Options`：网关门面；进程生命周期委托给 `httpserver.Run` |
| `inference.go` | `Inference`：每种 client format 一个 handler——模型授权（与 pipeline 阶段纵深防御）、router 候选、context-window 预过滤（413 `context_window_exceeded`，fail-open）、施加 tier、context 与 affinity 闸门的 fallback resolver、relay 执行与结算输入 |
| `admin.go` | admin 面 operations 半边：bearer 守卫、quota 读/充值、breaker 状态与重置、routing 视图、model/upstream 开关、insights 报告 |
| `identityadmin.go` | identity 半边：`/admin/users` 与 `/admin/keys` 子树；`AdminStore` 哨兵错误映射为 404/409/503 |
| `health.go` | liveness 与 readiness；readiness 以注入的探针为闸——即 fail-closed limiter 的依赖——基础设施细节只进日志，绝不进响应体 |
| `models.go` | `GET /v1/models`：以 OpenAI list 形式列出 client-facing 模型名；不鉴权、不含 tenant 数据、构造时渲染一次 |

## Tests

每个面一个文件：三种 inference format 及其守卫、context 过滤器、admin
operations 与 identity 端点、routing 开关端点、探针与 version。

## Invariants

- 组合只发生在 `cmd/*` 根中；本包装配路由，但不持有装配策略。
- `/admin/` mux 模式刻意不做 method 限定：admin handler 自己守卫方法——若在
  mux 层限定方法，PUT 会被 mux 以 405 弹回，充值端点永远看不到它。
- readiness 必须以"缺席即改变降级姿态"的依赖为闸——limiter 是 fail-closed
  的，绕过后端的 readiness 就是在说谎。探针失败时细节进日志，响应体只答固定
  短语。
- inference chain 在 observation stage 内部保留一份内层 recovery：在那里
  panic 会被计为 500 gateway fault，而不是 client disconnect。
- `GET /v1/models` 刻意保持不鉴权、无 tenant 数据：发现端点不得泄露
  governance 阶段守着的东西。
