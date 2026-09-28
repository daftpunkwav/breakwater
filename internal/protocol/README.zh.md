# protocol/

> 语言：**简体中文** | [English](README.md)

网关的 wire 契约层：把每一种客户端 API 格式 ingest 进同一个 canonical OpenAI-chat 形态，再把响应渲染回各自的格式。governance 与 routing 只看 canonical 形态，格式差异绝不越过本包边界。HTTP endpoint 形态（路由、handler）属于 `internal/server`，不在这里。

## Files

| 文件 | 职责 |
|---|---|
| `wire.go` | 翻译契约：`Format` 枚举、`Ingest`、`Wire` 与 `StreamTranscoder` 接口、`WireFor`。`chatWire` 即 canonical wire 本身——byte passthrough，只转发 `Content-Type` 与 `Retry-After`，越界的上游 status 收敛为 502 |
| `schema.go` | chat completion schema 的解码子集（`ChatRequest`、`Usage`）、`ParseUsage`、网关错误信封（`WriteError`）、共享拒绝码（`CodeRateLimited`、`CodeInsufficientQuota`、`CodeModelNotAllowed`）以及 `MaxBodyBytes`（4 MiB） |
| `sse.go` | SSE codec：`WriteData`/`WriteEvent`、in-stream 错误契约（`ErrorType` = `gateway_error`，码为 `upstream_reset` / `upstream_timeout` / `budget_exhausted`）、`WriteAbort`——恰好一个 error event，随后 `data: [DONE]` |
| `responses.go` | openai-responses wire：ingest（`input`、`instructions`、`max_output_tokens`）、Responses 对象、`response.*` 事件、中断时输出 `response.failed` |
| `anthropic.go` | anthropic-messages wire：ingest（`system`、text block、必填 `max_tokens`）、Messages 对象、`message_*` 事件、中断时输出 `error` 事件，以及 stop-reason 与 error-type 映射 |

## Invariants

- 一种格式一个文件、一个测试文件——translator-matrix 纪律。
- canonical wire 不丢任何东西：请求字节原样转发，未知字段永不重新编码。translated wire 只 ingest 声明过的子集，对无法诚实表达的内容（`tools`、`tool_choice`、`reasoning`、`previous_response_id`、`stop_sequences`、`top_k`、非文本 block）大声拒绝，而不是悄悄丢弃。
- translated 流只支持文本，且 settlement 依赖最终 usage chunk，因此 ingest 会向 canonical 请求注入 `stream_options.include_usage`——这是网关内部管道，客户端无需感知。
- translated 表面的对象 id 由网关生成（前缀 `resp_gw_`、`msg_gw_`），因为上游 id 要到后续帧才揭晓。
- `Code` 是封闭集合，由 HTTP 错误信封与 in-stream 契约共享；Messages wire 把它映射到自己的 error-type 词汇。新增 code 属于协议变更，必须与 `sse_test.go` 的契约测试同步更新。
- SSE 字节布局由契约测试钉死，因为 SDK 会解析它；response header 与 flush 是 handler 的事，不归这些 codec。

消费方：[internal/server](../../internal/server/)；测试分类法见 [docs/TESTING.md](../../docs/TESTING.md)。
