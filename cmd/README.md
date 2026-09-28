# cmd/

> Language: **English** | [简体中文](README.zh.md)

The two binaries. Per the root README's layout zoning, composition happens
exclusively in `cmd/*` roots: a root owns the wiring (which store, which
backend, which options), while every `internal/*` package stays a capability
leaf.

| Binary | Role | README |
| --- | --- | --- |
| [`breakwater/`](breakwater/) | The gateway binary: the composition root assembling governance backends, the three inference chains and the admin surface from `BREAKWATER_*` configuration | [`breakwater/README.md`](breakwater/README.md) |
| [`mockllm/`](mockllm/) | The mock OpenAI-compatible upstream with fault injection; the CLI wrapper over [`internal/mockllm`](../internal/mockllm/) | none — two files, documented here |

## mockllm/

`main.go` parses three flags and hands them to `internal/mockllm.New`, then
serves through the shared `httpserver.Run` lifecycle (10s shutdown grace):

| Flag | Default | Effect |
| --- | --- | --- |
| `-addr` | `:8090` | Listen address |
| `-default-delay` | `0` | Delay injected into every request |
| `-error-rate` | `0` | Fraction of requests answered with 500 (0..1) |

Per-request fault directives ride headers (`X-Mockllm-*`); the full
directive table and example scenarios live in the root README's "Fault
injection interface" section and in [`loadtest/`](../loadtest/).
