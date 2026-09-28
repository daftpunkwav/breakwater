# protocol/

> Language: **English** | [简体中文](README.zh.md)

Wire contracts of the gateway: how each client-facing API format is ingested into one canonical OpenAI-chat form, and how responses are rendered back out. Governance and routing see only the canonical form; format differences never leak past this package. HTTP endpoint shapes (routes, handlers) belong to `internal/server`, not here.

## Files

| File | Role |
|---|---|
| `wire.go` | The translation contract: `Format` enum, `Ingest`, the `Wire` and `StreamTranscoder` interfaces, `WireFor`. `chatWire` is the canonical wire itself — byte passthrough, only `Content-Type` and `Retry-After` forwarded, an out-of-range upstream status clamped to 502 |
| `schema.go` | The decoded subset of the chat completion schema (`ChatRequest`, `Usage`), `ParseUsage`, the gateway error envelope (`WriteError`), the shared rejection codes (`CodeRateLimited`, `CodeInsufficientQuota`, `CodeModelNotAllowed`) and `MaxBodyBytes` (4 MiB) |
| `sse.go` | SSE codecs: `WriteData`/`WriteEvent`, the in-stream error contract (`ErrorType` = `gateway_error`, codes `upstream_reset` / `upstream_timeout` / `budget_exhausted`), `WriteAbort` — exactly one error event, then `data: [DONE]` |
| `responses.go` | The openai-responses wire: ingest (`input`, `instructions`, `max_output_tokens`), Responses objects, `response.*` events, `response.failed` on abort |
| `anthropic.go` | The anthropic-messages wire: ingest (`system`, text blocks, required `max_tokens`), Messages objects, `message_*` events, the `error` event on abort, stop-reason and error-type mapping |

## Invariants

- One file per format, one test file per format — the translator-matrix discipline.
- The canonical wire loses nothing: request bytes forward verbatim, unknown fields are never re-encoded. Translated wires ingest a declared subset and refuse what they cannot express honestly (`tools`, `tool_choice`, `reasoning`, `previous_response_id`, `stop_sequences`, `top_k`, non-text blocks) instead of silently dropping it.
- Translated streams are text-only and their settlement depends on the final usage chunk, so ingest injects `stream_options.include_usage` into the canonical request — gateway-internal plumbing the client never sets.
- Object ids on translated surfaces are gateway-generated (`resp_gw_`, `msg_gw_` prefixes) because upstream ids are not known until later frames.
- `Code` is a closed set shared by the HTTP error envelope and the in-stream contract; the Messages wire maps it onto its own error-type vocabulary. Adding a code is a protocol change and must update the contract tests in `sse_test.go` together with the enum.
- SSE byte layout is pinned by contract tests because SDKs parse it; response headers and flushing are the handler's business, not the codecs'.

Consumed by [internal/server](../../internal/server/); the testing taxonomy lives in [docs/TESTING.md](../../docs/TESTING.md).
