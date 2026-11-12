# internal/protocol/ agent rules

Shared rules: [../AGENTS.md](../AGENTS.md). Package map: [README.md](README.md).

- Governance and routing see the canonical OpenAI-chat form. Format
  differences do not leave this package.
- HTTP routes and handlers belong to `internal/server`.
- One file per client format, and one test file per format.
- The canonical wire forwards request bytes unchanged. Do not re-encode
  unknown fields. Forward only `Content-Type` and `Retry-After`. Relay a
  `Content-Type` naming a document a browser would render — HTML, XHTML,
  SVG, XML — as `text/plain` (`ForwardedContentType`), and never leave
  the media type unset, which would hand the label to net/http's
  sniffing of upstream bytes. Clamp an out-of-range upstream status to
  502.
- Translated wires ingest a declared subset. Refuse fields and parts
  they cannot express, including `tools`, `tool_choice`, `reasoning`,
  `previous_response_id`, `stop_sequences`, `top_k`, and non-text
  blocks. Do not drop them.
- Translated streaming ingest sets `stream_options.include_usage` on
  the canonical request.
- Translated object ids use the `resp_gw_` and `msg_gw_` prefixes.
- `protocol.Code` is the closed failure vocabulary. Governance
  constants, pinned by `TestRejectionCodeValues`, are
  `rate_limit_exceeded`, `insufficient_quota`, `model_not_allowed`,
  `missing_api_key`, `invalid_api_key`, `identity_unavailable`,
  `concurrency_limit_exceeded`, `quota_not_provisioned`, and
  `governance_unavailable`. In-stream constants, pinned by
  `TestWriteAbortAllCodes`, are `upstream_reset`, `upstream_timeout`,
  and `budget_exhausted`. Gateway failure and routing constants,
  pinned by `TestGatewayFailureCodeValues`, are `no_upstream`,
  `circuit_open`, `upstream_saturated`, `upstream_unreachable`,
  `model_disabled`, `model_not_found`, `context_window_exceeded`, and
  `pipeline_misconfigured`; `budget_exhausted` is shared with the
  in-stream set.
- A new governance constant updates `rejection_codes_test.go`. A new
  in-stream constant updates `sse_test.go`. A new gateway failure
  constant also updates `rejection_codes_test.go`. The Messages wire
  maps `Code` values onto its own error-type vocabulary.
- `WriteAbort` writes one error event (`error` type `gateway_error`)
  and then `data: [DONE]`. In-stream codes are `upstream_reset`,
  `upstream_timeout`, and `budget_exhausted`.
- SSE codecs do not set response headers and do not flush. Handlers do.
- `MaxBodyBytes` is 4 MiB.
- `EstimateContentTokens` is the shared content measure: the gateway's
  reservation and the mock upstream's accounting must agree on it, or
  a settlement refunds a tenant down to a number it never spent. One
  token is one whitespace-separated word, floored by the
  four-bytes-per-token rule, so a content without whitespace cannot
  price as a single token.
- `WriteError` and `RenderError` take `string(<constant>)` — never a
  fresh literal. The one exception is the request-shape pair
  `invalid_request` and `request_too_large`, the body/admin surface
  codes that are not failure-taxonomy codes.
