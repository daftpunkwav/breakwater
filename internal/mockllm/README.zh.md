# mockllm/

> 语言：**简体中文** | [English](README.md)

进程内的 mock OpenAI 兼容上游，是网关测试与实验的故障注入器。happy path 保持 OpenAI-chat 形态（非流式 JSON 与 SSE chunk 流），被测网关不需要任何特殊处理，故障叠加在其上。把这个库变成进程的 CLI 包装是 [cmd/mockllm](../../cmd/mockllm/)（flag：`-addr`、`-default-delay`、`-error-rate`）；两者相关但不同——测试直接内嵌 handler，二进制额外负责 flag、信号与生命周期。

## Files

| 文件 | 职责 |
|---|---|
| `handler.go` | `Handler`/`Options`/`ServeHTTP`：路由 `POST /v1/chat/completions` 与 `GET /healthz`（方法错误 → 405，未知路径 → 404），应用进程级旋钮（`DefaultDelay`、`ErrorRate` → 500 `injected_failure`），请求体上限 1 MiB |
| `faults.go` | 每请求的 `X-Mockllm-*` 指令：延迟、注入 status（400..599）、`StreamMode`（`normal`/`slow`/`abort`）、chunk 间隔、省略 usage、补全长度。`slow` 模式未显式给 chunk 间隔时默认 100 ms |
| `completion.go` | OpenAI 兼容渲染：确定性补全（一个 token = 一个空白分隔的单词，来自固定词表；默认 32 tokens，header 上限 100000）、带最终 usage chunk 的 SSE chunk 流、OpenAI 错误信封 |
| `health.go` | `GET /healthz` 存活探针，供 compose 健康检查与实验脚本使用 |

## Invariants

- happy path 保持 OpenAI 兼容：mock 绝不迫使网关添加特殊处理。
- 内容刻意保持确定性——固定词表、固定长度——让 load test 各轮可比较。
- 故障指令大声失败：malformed 或越界的 `X-Mockllm-*` header 返回 `400 invalid_fault_directive`，因为悄悄忽略自身注入的实验只会产出无意义的证据。
- `abort` 模式在流中途切断连接（panic `http.ErrAbortHandler`）——没有错误信封、没有 `[DONE]`：正是网关的诚实流终止必须存活的截断流。
- 故障按固定顺序生效：先注入延迟（感知 context，客户端断连即提前结束），再掷进程级 error rate，最后套用每请求的 status 覆盖。

完整的故障注入 header 表见根 [README](../../README.md)；消费此 mock 的场景见 [loadtest/](../../loadtest/)。
