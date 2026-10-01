# internal/server/ agent rules

Shared rules: [../AGENTS.md](../AGENTS.md). Route map: [README.md](README.md).

## Scope

- This package owns HTTP endpoint shapes: route assembly, the three
  inference endpoints, admin, probes, and discovery.
- Wire formats belong to `internal/protocol`. Listen, serve, and drain
  belong to `internal/httpserver`.
- Do not select stores, backends, or process wiring here.

## Routes

- `POST /v1/chat/completions`, `POST /v1/responses`, `POST /v1/messages`.
  An unknown format constant maps to the chat route.
- `GET /v1/models` is unauthenticated and contains no tenant data.
- `GET /healthz`, `GET /readyz`, `GET /metrics`, and `GET /version` do
  not adopt an `X-Request-Id`.
- Mount admin on `/admin/` with no method in the pattern. The admin
  handler checks the method.
- When `BREAKWATER_ADMIN_TOKEN` is non-empty, `/admin/*` requires that
  bearer. Compare SHA-256 digests. An empty token allows every caller.
- Admin JSON bodies reject unknown fields and trailing values with 400
  `invalid_request`.

## Inference

- Call `AuthorizeModel` in the handler as well as in the pipeline.
- Wrap the whole mux in a recovery stage. That stage renders a 500 for
  routes that have no observation stage.
- The inference chain's `RecoveryStage` sits inside `ObservationStage`.
  A panic there is a counted 500.
- Map `router.ErrDisabled` to 403 `model_disabled`,
  `router.ErrUnavailable` to 503 `circuit_open`, and no binding to 404
  `model_not_found`.
- A model with no positive context ceiling is not rejected for size.
  A prompt over the primary model's ceiling returns 413
  `context_window_exceeded` and does not walk fallbacks.
- The fallback resolver applies tier authorization, then the context
  ceiling. Omit a fallback the tenant cannot use.
- Readiness uses the injected probe. Log the failure detail. The body
  stays a fixed phrase.
- `AdminStore` sentinel errors map to 404, 409, and 503.
