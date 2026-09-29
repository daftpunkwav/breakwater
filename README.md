# Breakwater

> Language: **English** | [简体中文](README.zh.md)

An LLM gateway written in Go that serves three client-facing API formats —
**OpenAI Chat Completions** (`/v1/chat/completions`), **OpenAI Responses**
(`/v1/responses`) and **Anthropic Messages** (`/v1/messages`) — over one
canonical governance pipeline, built to prove one thesis: high-concurrency
reliability governance — rate limiting, circuit breaking with failover,
bounded retry, cache stampede protection, lease-based quota consistency and
asynchronous observability — implemented by hand and backed by reproducible
tests, metrics and fault-injection experiments.

## Components

| Path                  | Responsibility                                            |
| --------------------- | --------------------------------------------------------- |
| `cmd/breakwater`      | Gateway binary (composition root)                         |
| `cmd/mockllm`         | Mock OpenAI-compatible upstream with fault injection      |
| `internal/server`     | Route assembly, the three inference endpoints, admin API (operations + identity administration) |
| `internal/relay`      | Response-side execution engine: attempts, failover, SSE passthrough, honest stream termination |
| `internal/pipeline`   | Middleware chain, per-request carrier, model authorization, observation stage |
| `internal/httpserver` | Shared HTTP lifecycle and the response tee                |
| `internal/insights`   | Monitoring record store: batched async writes, stability aggregation (success rate, failure mix, percentiles, timelines) |
| `internal/protocol`   | Wire contracts: the canonical chat form, the translator wires (openai-chat passthrough, openai-responses and anthropic-messages transcoding), SSE codecs |
| `internal/auth`       | Identity: users, roles, layered key limits (static/PostgreSQL stores, process-local LRU) |
| `internal/limiter`    | RPM/TPM token buckets (in-memory + Redis Lua), per-tenant concurrency gate, 429 stages |
| `internal/quota`      | Lease ledger (in-memory + Redis Lua), sweeper, 402 stage  |
| `internal/cache`      | Exact-match cache, hand-written singleflight, eligibility |
| `internal/circuit`    | Breakers behind one port: three-state consecutive machine or windowed ratio guard (plus a nop for breaker-less runs) |
| `internal/retry`      | Attempt loop, budgets, retryability classifier            |
| `internal/router`     | Candidate selection: static priority or measured-latency order, breaker pre-filtering, runtime operator switches |
| `internal/upstream`   | Provider port + OpenAI-compatible adapter                 |
| `internal/config`     | Configuration schema and loading                          |
| `internal/obs`        | Bounded async access log (with the per-attempt trail), hand-written metrics registry |
| `deploy`              | docker-compose stack, schema, seed, container build       |
| `loadtest`            | k6 scenarios, each targeting one property of the system   |
| `docs`                | Benchmarks, the fault-injection report, the local deployment guide and the testing conventions |

## Layout zoning

The directory tree is closed: future growth lands as new files inside
existing packages, never as new top-level directories. The zoning rules:

1. Governance mechanisms are self-contained: implementations, Lua
   scripts, sweepers and their own pipeline middleware grow inside their
   package — never subpackages.
2. Provider adapters stay flat: every OpenAI-compatible provider is served
   by `internal/upstream/openai.go` configured with a different
   `base_url`. The package holds one file per provider concern (`upstream.go`,
   `openai.go`, `modelmap.go`, `transport.go`), not an adapter tree.
3. Everything HTTP-endpoint-shaped belongs to `internal/server`
   (business `/v1/*`, `/admin/*`, probes); everything wire-format-shaped
   belongs to `internal/protocol`.
4. `internal/httpserver` hosts only neutral, stdlib-only transport
   mechanics; composition happens exclusively in `cmd/*` roots.

## Client formats

| Route                      | Format                | Auth header              | Notes |
| -------------------------- | --------------------- | ------------------------ | ----- |
| `POST /v1/chat/completions`| OpenAI Chat (canonical)| `Authorization: Bearer` | Byte passthrough to OpenAI-compatible upstreams; SSE passthrough |
| `POST /v1/responses`       | OpenAI Responses      | `Authorization: Bearer`  | Translated: input/instructions/max_output_tokens in, Responses objects and `response.*` events out |
| `POST /v1/messages`        | Anthropic Messages    | `x-api-key` or Bearer    | Translated: system/blocks/required `max_tokens` in, Messages objects and `message_*` events out; `stop_sequences` refused rather than silently dropped |

Every inference response carries an `X-Request-Id` header: a well-formed
client-supplied id is adopted verbatim, otherwise one is minted
(`req-` prefix). The id travels to the upstream exchange and into the
access log, so one identifier joins the client-visible outcome, the
gateway's log line and the provider's records. The correlation stage
belongs to the inference chain: the admin, discovery and probe routes do
not carry one.

`GET /v1/models` lists the client-facing model names in the OpenAI
list form, so OpenAI-compatible clients can discover what to ask for.
The endpoint is unauthenticated and carries no tenant data.

### Model names and aliases

The gateway routes on the model name the client sends. A `models`
entry of the form `"client=real"` serves the client-facing name by
forwarding the provider-real name — the adapter rewrites the request
body's `model` field, everything else passes through verbatim:

    "models": ["claude-sonnet=claude-sonnet-4-20250514", "deepseek-chat"]

With the same client-facing name bound to several upstreams, one
request carries its own failover chain — and because each upstream
rewrites to its own real name, a mid-request failover can cross
providers and models, not just hosts.

Chains can extend across models: `BREAKWATER_FALLBACKS` maps a model
to fallback models tried in order once its own candidates are
exhausted — each hop re-resolves candidates, rewrites the forwarded
body's model name, and never re-enters a model already tried. The
chain is a preference list, not a contract: a hop that cannot serve
(disabled, unbound, prompt over its context ceiling) is skipped.

All three walk the identical governance pipeline (auth → model
authorization → concurrency → rate limit → quota → cache) and the identical relay engine; only the wire differs.
The canonical wire is openai-chat: unknown request fields survive byte
passthrough, while translated formats ingest a declared subset and
reject what they cannot express honestly. Translated formats take text
only: a non-text input part or message block is rejected at ingest. The
canonical wire forwards verbatim, so tool declarations and tool messages pass
through untouched, while a multimodal `content` array fails the canonical decode
and is rejected as a malformed request. The exact-match cache serves the
canonical wire (translated replays would need response re-rendering,
which is deliberately not faked).

## Quick start

Start the mock upstream and the gateway with the in-memory governance
backends (no Redis needed for a smoke run):

    go run ./cmd/mockllm -addr 127.0.0.1:8090

    BREAKWATER_UPSTREAMS='[{"id":"mock","base_url":"http://127.0.0.1:8090","probe_url":"http://127.0.0.1:8090/healthz","models":["*"]}]' \
    BREAKWATER_IDENTITY='{"tiers":[{"id":"free","rpm":60,"tpm":200000,"max_tokens":4096,"monthly_quota":10000000,"allowed_models":["*"]}],"tenants":[{"id":"local","name":"Local","tier":"free","keys":["bw-local-dev-key"]}]}' \
    BREAKWATER_ADMIN_TOKEN='dev-admin' \
    go run ./cmd/breakwater

Send a completion through the gateway:

    curl -s http://127.0.0.1:8080/v1/chat/completions \
      -H 'Authorization: Bearer bw-local-dev-key' \
      -H 'Content-Type: application/json' \
      -d '{"model":"mock-gpt","messages":[{"role":"user","content":"hi"}]}'

Stream one:

    curl -s -N http://127.0.0.1:8080/v1/chat/completions \
      -H 'Authorization: Bearer bw-local-dev-key' \
      -H 'Content-Type: application/json' \
      -d '{"model":"mock-gpt","stream":true,"messages":[{"role":"user","content":"hi"}]}'

With Redis configured (`BREAKWATER_REDIS_ADDR`), the limiter and quota
ledger run on atomic Lua scripts and the identity tier balances are
seeded automatically from the static identity set. With
`BREAKWATER_POSTGRES_DSN` configured, API keys resolve from PostgreSQL
(schema in `deploy/schema.sql`, local seed in `deploy/seed.sql`),
otherwise the `BREAKWATER_IDENTITY` JSON is the system of record.

## Configuration

All configuration is environment-based; core knobs:

| Variable                              | Default        | Effect                                                     |
| ------------------------------------- | -------------- | ---------------------------------------------------------- |
| `BREAKWATER_ADDR`                     | `:8080`        | Listen address                                             |
| `BREAKWATER_SHUTDOWN_GRACE`           | `15s`          | Drain window once shutdown begins; streams still in flight when it expires are cut, logged as a warning, and the process still exits successfully |
| `BREAKWATER_UPSTREAMS`                | _(none)_       | JSON list of upstreams (`id`, `base_url`, `probe_url`, `api_key`, `api_keys`, `models`; list order = failover priority; `"client=real"` entries alias model names; `api_keys` rotates several credentials behind one upstream). Unknown members and ids outside `[A-Za-z0-9._-]{1,128}` refuse to boot |
| `BREAKWATER_ROUTING_STRATEGY`         | `static`       | Candidate order: `static` (configured order) or `latency` (measured exchange latency first; near-tied candidates trade the lead per request, configured order breaks remaining ties; untried upstreams are explored first) |
| `BREAKWATER_FALLBACKS`                | _(none)_       | JSON map of model → ordered fallback models, tried when every candidate of the primary model is exhausted (`{"gpt-4o":["gpt-4o-mini"]}`); keys and targets must name configured client-facing models |
| `BREAKWATER_CONTEXT_LIMITS`           | _(none)_       | JSON map of model → maximum input token estimate; a prompt above the ceiling refuses that model's candidates up front with `413 context_window_exceeded` instead of a doomed upstream exchange |
| `BREAKWATER_IDENTITY`                 | _(none)_       | JSON identity set (`tiers`, `tenants` with `role` and user-level `overrides`; unknown members refuse to boot, tenant ids are `[A-Za-z0-9._-]{1,128}`); arms the governance pipeline. Also accepts `file://<path>` to load the JSON from a file, keeping raw API keys off the process environment |
| `BREAKWATER_POSTGRES_DSN`             | _(none)_       | Identity system of record (overrides the static set)       |
| `BREAKWATER_REDIS_ADDR`               | _(none)_       | Enables the Redis backends; without it, in-memory          |
| `BREAKWATER_REDIS_TLS`                | _(off)_        | Wraps the Redis connection in TLS; the certificate must verify against the system roots with the address host as the server name. Balances and rate limit state cross the wire in plaintext otherwise |
| `BREAKWATER_ALLOW_UNAUTHENTICATED`    | _(off)_        | Explicit dev opt-in permitting a refused posture: upstreams without any identity source (an unauthenticated open proxy), a PostgreSQL identity without `BREAKWATER_ADMIN_TOKEN` (an open key-minting surface), or any armed configuration (upstreams, identity or insights) without `BREAKWATER_ADMIN_TOKEN` (an open admin surface: balance writes, breaker resets, routing switches). All refuse to boot without this |
| `BREAKWATER_REDIS_NAMESPACE`          | _(none)_       | Prefixes every limiter and quota key. Set it whenever two deployments share one Redis instance — otherwise they share tenant balances, and one environment's sweeper refunds the other's live leases. Empty assumes this gateway owns its Redis outright. **Setting it on a deployment that already has balances abandons every existing key (nothing is migrated), and the static-identity seeder then re-provisions each tenant at its full monthly budget** — treat it as a reset, not a rename. A PostgreSQL-identity deployment has no seeder, so there a namespace change leaves every tenant without a ledger until one is provisioned. |
| `BREAKWATER_QUOTA_LEASE_TTL`          | _derived_      | How long a reserved quota lease may live before the sweeper reclaims and refunds it. Derived from `OverallDeadline + StreamTimeout` plus headroom, because a horizon shorter than the longest request silently refunds a request that really spent tokens; a value below that is rejected at startup. An unbounded stream or deadline leaves nothing to derive from, so those default to 24h. |
| `BREAKWATER_RETRY_MAX_ATTEMPTS`       | `3`            | Upstream attempts per request                              |
| `BREAKWATER_RETRY_ATTEMPT_TIMEOUT`    | `30s`          | Time-to-first-byte ceiling of one upstream attempt; `0` uncaps |
| `BREAKWATER_RETRY_OVERALL_DEADLINE`   | `60s`          | Ceiling on all attempts of one request together (failover and retries share it); `0` uncaps |
| `BREAKWATER_RETRY_BACKOFF_INITIAL`    | `100ms`        | The first retry waits at most this long (full jitter); the wait ceiling doubles with every later attempt |
| `BREAKWATER_RETRY_BACKOFF_MAX`        | `2s`           | Upper bound of the backoff wait ceiling; `0` leaves the doubling uncapped |
| `BREAKWATER_RETRY_BUDGET_MAX_IN_FLIGHT` | `64`         | Process-wide concurrent retry cap; superseded when a share budget is configured |
| `BREAKWATER_RETRY_BUDGET_PERCENT`     | _(off)_        | Share budget: retries may occupy at most this percentage of the requests currently in flight — the cap scales with live traffic instead of one static number. `0` keeps the fixed cap. |
| `BREAKWATER_RETRY_BUDGET_MIN_IN_FLIGHT` | `3`          | The share budget's floor: the retry cap never drops below it however quiet the gateway is |
| `BREAKWATER_STREAM_TIMEOUT`           | `10m`          | Ceiling of a committed stream's body (after the headers); `0` lets the client own the stream's lifetime. Keep it looser than the attempt timeout, which bounds time-to-first-byte: otherwise a slow header is cut by this ceiling, and the request fails over instead of waiting for it. |
| `BREAKWATER_STREAM_IDLE_TIMEOUT`      | _(off)_        | Idle watchdog of a committed stream: an upstream that stops producing for the whole window loses the stream (same honest error frame) even while the ceiling would still allow it. `0` disables — thinking models can legitimately stay silent for minutes between tokens. |
| `BREAKWATER_CACHE_ENABLED` / `_TTL` / `_CAPACITY` | on / `60s` / `1024` | Exact-match response cache      |
| `BREAKWATER_CIRCUIT_STRATEGY`         | `consecutive`  | `consecutive` opens after `FAIL_THRESHOLD` consecutive server faults and re-admits one probe after `COOLDOWN`; `ratio` denies a rising share of calls computed from a rolling 10s window of outcomes and always admits one call per second, so it never fully cuts traffic (its denials surface on `breakwater_circuit_denied_total`; threshold and cooldown are consecutive-only) |
| `BREAKWATER_CIRCUIT_*`                | on / `5` / `30s` / `5s` | Breaker threshold, cooldown, probe timeout      |
| `BREAKWATER_PROBE_INTERVAL` / `_TIMEOUT` / `_THRESHOLD` / `_BACKOFF_MAX` | `30s` / `5s` / `2` / _(off)_ | Active recovery probing of out-of-rotation upstreams; interval `0` disables (recovery then waits for real traffic); an auto-disabled upstream is restored only after `threshold` consecutive healthy probes. `BACKOFF_MAX` spaces an auto-disabled upstream's probes out after failures (wait starts at one interval, doubles per consecutive failure, capped here; a healthy answer forgets the ladder); `0` keeps every tick probing |
| `BREAKWATER_ACCESS_LOG_PATH`          | _(off)_        | JSONL access log file (bounded queue, drop-oldest)         |
| `BREAKWATER_ACCESS_LOG_QUEUE_SIZE`    | `4096`         | Capacity of the access log's in-memory queue; overflow drops entries and counts the drops |
| `BREAKWATER_ADMIN_TOKEN`              | _(none)_       | Bearer token guarding `/admin/*`. Required on any armed deployment (upstreams, identity or insights configured); the unarmed zero-config run boots without it |
| `BREAKWATER_RECONCILE_INTERVAL`       | `1m`           | Quota ledger reconciliation pacing; needs Redis + PostgreSQL; `0` disables |
| `BREAKWATER_INSIGHTS_DSN`             | _(main DSN)_   | PostgreSQL the monitoring records persist to; defaults to `BREAKWATER_POSTGRES_DSN`; unset without any DSN |

## Operations surface

- `GET /healthz` — liveness
- `GET /readyz` — readiness (gates on the backend the fail-closed
  limiter depends on; it never lies)
- `GET /metrics` — Prometheus text exposition (hand-written registry)
- `GET /version` — build identifier and Go version
- `GET /admin/tenants/{id}/quota` — current balance
- `PUT /admin/tenants/{id}/quota` — top-up or correct a balance (`{"balance": N}`); a PUT to an id with no balance creates it (upsert); the reconcile protocol treats the interval across a correction as skip-by-design
- `GET /admin/breakers` — per-upstream breaker states
- `POST /admin/breakers/{id}/reset` — force an open breaker closed
- `POST /admin/upstreams/{id}/probe` — one health probe on demand
- `GET /admin/insights?hours=N` — the stability report for the trailing window (default 24): success rate, failure mix by cause, latency percentiles, a 5-minute timeline and per-tenant/key/model/upstream breakdowns (top 20 rows each, busiest first). Requires the monitoring store (any PostgreSQL DSN).
- `GET /admin/routing` — every known model and upstream with its current eligibility, plus the auto-disabled upstreams with the reason and moment of each decision
- `PUT /admin/models/{id}` — enable or disable a model (`{"enabled": false}`); disabled models refuse requests with `403 model_disabled`
- `PUT /admin/upstreams/{id}` — enable or disable an upstream; disabled upstreams drop out of every candidate list. Switches are in-memory and reset on restart. An operator enable clears both disable channels (see below).

### Automatic upstream recovery

Beyond the operator switches, the gateway keeps upstreams honest on
its own:

- A completed upstream exchange that proves a **fatal condition** —
  rejected credentials (401, or a 403 whose provider error envelope is
  credential-class — type `authentication_error` or code `invalid_api_key`; a
  403 for model access, region or content policy stays retryable) or an
  exhausted budget (`insufficient_quota`, which OpenAI
  reports as a 429) — takes the upstream out of rotation immediately,
  with the reason recorded in `/admin/routing` and counted in
  `breakwater_upstream_auto_disabled_total`.
- An upstream 429 carrying a `Retry-After` header delays the next
  retry **of that same upstream**; failover to a different candidate
  never waits for the failed one's hint.
- The recovery loop (if `BREAKWATER_PROBE_INTERVAL` is non-zero and
  the upstream declares a `probe_url`) periodically probes
  auto-disabled and breaker-ejected upstreams; restoring an
  auto-disabled upstream takes `BREAKWATER_PROBE_THRESHOLD`
  consecutive healthy probes (one failure resets the count), so a
  flapping upstream cannot cycle back in. With
  `BREAKWATER_PROBE_BACKOFF_MAX` set, a failed probe spaces the next
  one out — the wait starts at one interval and doubles per
  consecutive failure up to the ceiling, so a hard-down upstream is
  asked ever more rarely; a healthy answer forgets the ladder, and
  `0` (the default) keeps every tick probing. Only the system's own
  disables are lifted this way — an operator disable survives until an
  operator lifts it. Point `probe_url` at an authenticated endpoint if
  you want credential and quota failures to self-heal.
- Two operator handles complete the loop: `POST
  /admin/breakers/{id}/reset` forces an open breaker closed ("I fixed
  the upstream, let it through now"), and `POST
  /admin/upstreams/{id}/probe` runs one health exchange on demand
  (200 healthy, 409 when the upstream declares no `probe_url`, 502
  when the probe fails).

### Credential rings

An upstream may serve several provider credentials at once — `api_keys`
lists further bearer tokens that rotate behind the same `base_url`
(`api_key`, when set, leads the ring). Provider rate limits are per
credential, so the ring spreads the upstream's traffic across all of
them: each exchange takes the next alive credential, concurrent
requests land on different ones, and a credential this request saw
fail with a rate limit or a fatal condition is not reused within that
request.

A fatal condition (rejected credentials, exhausted budget) convicts the
credential, not the upstream: the ring retires it — counted in
`breakwater_credential_retired_total` — and the rest keep serving. Only
the last living credential's death takes the upstream out of rotation
through the same auto-disable as before, and whatever lifts that
disable (an operator enable or a healthy recovery probe) restores the
full ring. A retired credential itself comes back only with the
upstream's re-entry; there is no per-credential probe.

The attempt budget still bounds the walk: a request tries at most
`BREAKWATER_RETRY_MAX_ATTEMPTS` exchanges, so size it to cover the ring
when you want a fatal walk to reach every credential.

### Identity administration (PostgreSQL deployments)

With `BREAKWATER_POSTGRES_DSN` configured, identities are users with
keys — up to five per user — and each level carries its own limits
layer:

- `POST /admin/users` — create a user (`{"name", "tier", "role"}`; role `user` or `admin`)
- `GET /admin/users` — list users
- `PUT /admin/users/{id}/limits` — the user-level limits layer, applied to every key the user holds
- `POST /admin/users/{id}/keys` — issue a key (raw secret shown once)
- `GET /admin/users/{id}/keys` — list the user's keys
- `PUT /admin/keys/{id}/limits` — the key-level limits layer
- `PUT /admin/keys/{id}/status` — disable or re-enable one key (`{"enabled": false}`)

The limits layer (`LIMITS` below) is one JSON document:

```json
{"rpm": 30, "tpm": 50000, "max_tokens": 2048, "monthly_quota": 1000000,
 "concurrency": 4,
 "allowed_models": ["m1", "m2"],
 "denied_models": ["expensive-model"],
 "model_quotas": {"expensive-model": 100000}}
```

Resolution merges the tier template, the user layer and the key layer:
scalars take the nearest set value (key over user over tier);
`denied_models` union across layers — a user-level deny removes the
model from every key; `allowed_models` only ever tightens (an
`*`-wildcard tier plus a named user list means exactly that list);
`model_quotas` merge per model. An omitted field inherits; `{}` clears
the layer. Changes surface to live traffic within the auth cache TTL.
The static `BREAKWATER_IDENTITY` mode supports `role` and a
tenant-level `overrides` document, but has no administration surface.

## Fault injection interface

Per-request directives via headers:

| Header                        | Effect                                                     |
| ----------------------------- | ---------------------------------------------------------- |
| `X-Mockllm-Delay-Ms`          | Delay before the first response byte                       |
| `X-Mockllm-Status`            | Respond with this error status (400..599)                  |
| `X-Mockllm-Stream-Mode`       | `normal` (default) / `slow` / `abort`                      |
| `X-Mockllm-Chunk-Delay-Ms`    | Inter-chunk delay in `slow` mode                           |
| `X-Mockllm-Omit-Usage`        | Drop the usage field from responses                        |
| `X-Mockllm-Completion-Tokens` | Fixed completion length in tokens (words)                  |

Process-wide chaos knobs: `-default-delay`, `-error-rate`.

Example: a stream that dies mid-flight — the gateway keeps the delivered
chunks and terminates honestly with one in-stream error event and
`[DONE]`:

    curl -s -N http://127.0.0.1:8090/v1/chat/completions \
      -H 'X-Mockllm-Stream-Mode: abort' \
      -H 'Content-Type: application/json' \
      -d '{"model":"mock-gpt","stream":true,"messages":[{"role":"user","content":"hi"}]}'

## Developer tasks

A Makefile wraps the common loop (`make test`, `make race`, `make
cover`, `make lint`, `make build`, `make up`). `make build` injects the
git-derived version into the binaries.

## Local stack

    docker compose -f deploy/docker-compose.yml up -d

Starts Redis, PostgreSQL (identity schema and local seed applied on
first boot), the mock upstream and the gateway itself. Send a request
through the composed gateway:

    curl -s http://127.0.0.1:8080/v1/chat/completions \
      -H 'Authorization: Bearer bw-local-dev-key' \
      -H 'Content-Type: application/json' \
      -d '{"model":"mock-gpt","messages":[{"role":"user","content":"hi"}]}'

## Quality gate

CI enforces `gofmt`, `go vet`, `golangci-lint` and `go test -race ./...`
(with Redis and PostgreSQL service containers for the Lua and identity
integration tests). Every package meets the 95% statement-coverage bar in
that environment; a local run without Redis and PostgreSQL reads lower for
the three packages whose SQL paths sit behind the database-gated tests,
and `docs/TESTING.md` lists the measured numbers for both.
The taxonomy (unit / functional / integration / concurrency /
benchmark) and the naming rules live in `docs/TESTING.md`. Reliability
mechanisms (token bucket, circuit breaker, singleflight, rate limiting) are
implemented in this repository by discipline; the four matching third-party
libraries are denied at lint time by the `no-off-the-shelf-governance` rule.

## Documentation

Evidence methodology and scenario sets live in `docs/`:
`docs/BENCHMARK.md` (load test numbers), `docs/CHAOS-REPORT.md`
(fault-injection timelines) and `docs/DEPLOY-LOCAL.md` (wiring real
providers and agent applications into a local gateway). BENCHMARK and
CHAOS-REPORT are filled from real runs of the `loadtest/` scenarios —
see `loadtest/README.md`.

## License

MIT — see [LICENSE](LICENSE).
