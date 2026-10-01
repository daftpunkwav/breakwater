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
- `protocol.Code` governance constants, pinned by
  `TestRejectionCodeValues`, are `rate_limit_exceeded`,
  `insufficient_quota`, `model_not_allowed`, `missing_api_key`,
  `invalid_api_key`, `identity_unavailable`,
  `concurrency_limit_exceeded`, `quota_not_provisioned`, and
  `governance_unavailable`. In-stream constants, pinned by
  `TestWriteAbortAllCodes`, are `upstream_reset`, `upstream_timeout`,
  and `budget_exhausted`.
- A new governance constant updates `rejection_codes_test.go`. A new
  in-stream constant updates `sse_test.go`. The Messages wire maps
  `Code` values onto its own error-type vocabulary.
- Handlers also pass literal codes to `WriteError`, including
  `model_disabled`, `circuit_open`, `model_not_found`,
  `context_window_exceeded`, `upstream_unreachable`, `no_upstream`,
  and `invalid_request`.
- `WriteAbort` writes one error event (`error` type `gateway_error`)
  and then `data: [DONE]`. In-stream codes are `upstream_reset`,
  `upstream_timeout`, and `budget_exhausted`.
- SSE codecs do not set response headers and do not flush. Handlers do.
- `MaxBodyBytes` is 4 MiB.
- `WriteError` renders the HTTP error envelope. Governance stages
  pass `protocol.Code` values. Other handlers may pass the literal
  codes above.
