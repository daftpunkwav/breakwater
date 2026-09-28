# cmd/

> 语言：**简体中文** | [English](README.md)

两个二进制。按根 README 的 layout zoning，组合只发生在 `cmd/*` 根中：根拥有
装配（哪个 store、哪个 backend、哪些选项），而每个 `internal/*` 包保持为
能力叶子。

| 二进制 | 职责 | README |
| --- | --- | --- |
| [`breakwater/`](breakwater/) | 网关二进制：组合根，从 `BREAKWATER_*` 配置装配 governance backend、三条 inference chain 与 admin 面 | [`breakwater/README.md`](breakwater/README.md) |
| [`mockllm/`](mockllm/) | 带 fault injection 的 mock OpenAI-compatible upstream；[`internal/mockllm`](../internal/mockllm/) 的 CLI 包装 | 无——仅两个文件，在此处说明 |

## mockllm/

`main.go` 解析三个 flag，交给 `internal/mockllm.New`，再经共享的
`httpserver.Run` 生命周期对外服务（10s shutdown grace）：

| Flag | 默认值 | 作用 |
| --- | --- | --- |
| `-addr` | `:8090` | 监听地址 |
| `-default-delay` | `0` | 注入每个请求的延迟 |
| `-error-rate` | `0` | 以 500 应答的请求比例（0..1） |

每请求级的 fault 注入指令经 header（`X-Mockllm-*`）携带；完整指令表与示例
场景见根 README 的 "Fault injection interface" 一节和
[`loadtest/`](../loadtest/)。
