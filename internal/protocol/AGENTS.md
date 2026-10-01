# internal/protocol/ agent rules

Shared rules: [../AGENTS.md](../AGENTS.md). Package map: [README.md](README.md).

- Governance and routing see the canonical OpenAI-chat form. Format
  differences do not leave this package.
- HTTP routes and handlers belong to `internal/server`.
- One file per client format, and one test file per format.
- The canonical wire forwards request bytes unchanged. Do not re-encode
  unknown fields. Forward only `Content-Type` and `Retry-After`. Clamp
  an out-of-range upstream status to 502.
- Translated wires ingest a declared subset. Refuse fields and parts
  they cannot express, including `tools`, `tool_choice`, `reasoning`,
  `previous_response_id`, `stop_sequences`, `top_k`, and non-text
  blocks. Do not drop them.
- Translated streaming ingest sets `stream_options.include_usage` on
  the canonical request.
- Translated object ids use the `resp_gw_` and `msg_gw_` prefixes.
- `protocol.Code` constants are the governance rejections
  (`rate_limit_exceeded`, `insufficient_quota`, `model_not_allowed`)
  and the in-stream codes (`upstream_reset`, `upstream_timeout`,
  `budget_exhausted`). Handlers also pass literal codes to
  `WriteError`, including `model_disabled`, `circuit_open`,
  `model_not_found`, `context_window_exceeded`, `upstream_saturated`,
  `upstream_unreachable`, `no_upstream`, and `invalid_request`.
- Adding a `Code` constant updates `sse_test.go` and
  `rejection_codes_test.go`. The Messages wire maps `Code` values onto
  its own error-type vocabulary.
- `WriteAbort` writes one error event (`error` type `gateway_error`)
  and then `data: [DONE]`. In-stream codes are `upstream_reset`,
  `upstream_timeout`, and `budget_exhausted`.
- SSE codecs do not set response headers and do not flush. Handlers do.
- `MaxBodyBytes` is 4 MiB.
- `WriteError` is the gateway error envelope. Shared rejection codes
  include `rate_limit_exceeded`, `insufficient_quota`, and
  `model_not_allowed`.
