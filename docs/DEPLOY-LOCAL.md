# Local deployment: real providers, real clients

This is the operational path from a fresh checkout to a gateway that
serves your agent applications from your own providers. The mock
upstream remains available for fault injection (see the README), but
nothing here depends on it.

## 1. The shape of a deployment

    agent apps                breakwater (:8080)              providers
    ──────────                ──────────────────              ─────────
    zcode / CLI / IDE  ──▶   /v1/chat/completions   ──▶      provider A
    any OpenAI client  ──▶   /v1/responses          ──▶      provider B
    any Anthropic client ─▶  /v1/messages           ──▶      provider C
                             /admin/*  (ops)
                             /metrics /healthz /readyz /version

Clients speak their native format against one local port; breakwater
authenticates, rate-limits, meters quota, caches, then routes each
request across the configured upstreams with failover. The client
never sees a provider.

## 2. Choose your backends

- **In-memory (zero config)** — nothing to start; state is process-local.
  Fine for a single-user workstation.
- **Redis** (`BREAKWATER_REDIS_ADDR`) — the limiter buckets and quota
  ledger move to atomic Lua scripts; balances survive restarts.
- **PostgreSQL** (`BREAKWATER_POSTGRES_DSN`) — API keys resolve from the
  system of record (`deploy/schema.sql`) instead of the inline identity
  JSON, and the quota reconciliation protocol can arm.

For a local single-tenant setup the memory mode with an inline identity
is complete; add Redis when you want durable balances.

## 3. Wire in your providers

Providers are declared in `BREAKWATER_UPSTREAMS` (JSON list; list order
is the failover priority per model). Every OpenAI-compatible provider
— OpenAI, DeepSeek, Moonshot, OpenRouter, a local vLLM — uses the same
shape. Set a per-provider `api_key` and, when you want the gateway to
present a unified model namespace, alias with `client=real` entries:

    export BREAKWATER_UPSTREAMS='[
      {"id":"deepseek","base_url":"https://api.deepseek.com",
       "api_key":"sk-...",
       "models":["deepseek-chat","deepseek-reasoner"]},
      {"id":"openai","base_url":"https://api.openai.com",
       "api_key":"sk-...",
       "models":["gpt-4o","gpt-4o-mini",
                 "claude-sonnet=claude-sonnet-4-20250514"]},
      {"id":"fallback-pool","base_url":"https://openrouter.ai/api",
       "api_key":"sk-or-...",
       "models":["*"]}
    ]'

Reading the example: a client asking for `deepseek-chat` goes to
DeepSeek; `gpt-4o` goes to OpenAI; `claude-sonnet` goes to OpenAI's
aliased long name and, if that attempt fails, to OpenRouter's wildcard
pool — one request, two providers, each forwarding the model name its
provider actually serves. Wildcard (`*`) bindings need no rewrite.

Keys stay in your environment or secret store; nothing is written to
disk or logs. Probe URLs are optional — without one the breaker relies
on exchange outcomes alone.

## 4. Point your agent applications at the gateway

Any client that accepts a custom OpenAI-compatible base URL works
unchanged. Give it the gateway address and one of your tenant API keys
(the identity below continues the README quick start):

    BREAKWATER_IDENTITY='{"tiers":[
        {"id":"free","rpm":600,"tpm":900000,"max_tokens":4096,
         "monthly_quota":50000000,"allowed_models":["*"]}],
      "tenants":[{"id":"local","name":"Local","tier":"free",
         "keys":["bw-local-dev-key"]}]}' \
    go run ./cmd/breakwater

Then configure the agent (variable names per its documentation):

    OPENAI_BASE_URL=http://127.0.0.1:8080/v1
    OPENAI_API_KEY=bw-local-dev-key

Anthropic-native clients point at `http://127.0.0.1:8080` and speak
`/v1/messages` with `x-api-key: bw-local-dev-key`. Because all three
client formats share one pipeline, an agent may switch models per
request simply by naming a different model — the gateway routes,
meters and fails over whatever it receives.

## 5. Run the composed stack instead

    docker compose -f deploy/docker-compose.yml up -d --build

brings up Redis, PostgreSQL (schema + seed on first boot), the mock
upstream and the gateway. To use it with real providers, export the
`BREAKWATER_UPSTREAMS` you assembled above into the compose
environment for the `breakwater` service (or extend the `environment`
block in `deploy/docker-compose.yml`) before `up`.

## 6. Operate

| Need                                | Surface                                            |
| ----------------------------------- | -------------------------------------------------- |
| Is a provider failing right now?    | `GET /admin/breakers` and `GET /metrics`           |
| Take a misbehaving model out now    | `PUT /admin/models/{id}` `{"enabled": false}`      |
| Drain a provider (maintenance)      | `PUT /admin/upstreams/{id}` `{"enabled": false}`   |
| What is switched off?               | `GET /admin/routing`                               |
| Onboard a user (admin or employee)  | `POST /admin/users` `{"name","tier","role"}`       |
| Issue a key (max 5 per user)        | `POST /admin/users/{id}/keys` `{"name"}`           |
| Deny a model for a user             | `PUT /admin/users/{id}/limits` `{"denied_models":[...]}` |
| Tighten one key (quota/rpm/concurrency) | `PUT /admin/keys/{id}/limits` `{...}` |
| Revoke one leaked key               | `PUT /admin/keys/{id}/status` `{"enabled": false}` |
| Tenant balance / top-up             | `GET`/`PUT /admin/tenants/{id}/quota`              |
| Stability report (the assessment view) | `GET /admin/insights?hours=24` — success rate, failure causes, latency percentiles, per-tenant/key/model/upstream slices |
| Per-request audit trail             | `BREAKWATER_ACCESS_LOG_PATH` JSONL, keyed by `X-Request-Id` |

The monitoring store (`BREAKWATER_INSIGHTS_DSN`, defaulting to the
identity DSN) persists one row per finished request — including the
failure cause: governance rejections carry their code
(`invalid_api_key`, `rate_limited`, `quota_insufficient`,
`concurrency_limit_exceeded`), gateway failures theirs
(`circuit_open`, `budget_exhausted`, `upstream_unreachable`), and
upstream error passthroughs classify by status class. Client
disconnects never count as failures.

User and key management requires the PostgreSQL identity store; the
limits layers merge tier → user → key (nearest scalar wins, denies
union, allows only tighten) and take effect within the auth cache
TTL. Guard the admin surface with `BREAKWATER_ADMIN_TOKEN` whenever
the port is reachable beyond your own machine. Routing switches are
in-memory and reset on restart; permanent removal is a config change.

Set `BREAKWATER_ROUTING_STRATEGY=latency` to order same-model
candidates by measured exchange latency instead of config order — the
tracker scores every attempt (client cancels excluded) and untried
upstreams are explored first. Breakers and operator switches gate
eligibility in both modes; the strategy only orders.

## 7. Verify

    # one buffered call, any format
    curl -s http://127.0.0.1:8080/v1/chat/completions \
      -H 'Authorization: Bearer bw-local-dev-key' \
      -H 'Content-Type: application/json' \
      -d '{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}'

    # the same conversation in Anthropic format
    curl -s http://127.0.0.1:8080/v1/messages \
      -H 'x-api-key: bw-local-dev-key' \
      -H 'anthropic-version: 2023-06-01' \
      -H 'Content-Type: application/json' \
      -d '{"model":"claude-sonnet","max_tokens":64,
           "messages":[{"role":"user","content":"hi"}]}'

    # streaming
    curl -sN http://127.0.0.1:8080/v1/chat/completions \
      -H 'Authorization: Bearer bw-local-dev-key' \
      -H 'Content-Type: application/json' \
      -d '{"model":"deepseek-chat","stream":true,
           "messages":[{"role":"user","content":"hi"}]}'

Check `X-Request-Id` in a response against the access log line and the
provider's own request log; the same id should appear in all three.
