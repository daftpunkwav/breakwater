# Peer learning: patterns adopted from open-source gateways

This document records a source-level study of comparable open-source
projects and what breakwater took from each. Every finding below was
extracted by reading the actual source files (paths cited), not from
documentation or memory. Three statuses are used:

- **applied** — landed in this repository as part of this study round.
- **aligned** — breakwater already implements the same idea; the study
  confirmed the design against battle-tested peers.
- **roadmap** — worth borrowing later, deliberately not now.

Projects studied (default branches, as of 2026-09):

| Project | Language | What it is |
| --- | --- | --- |
| [songquanpeng/one-api](https://github.com/songquanpeng/one-api) | Go | LLM API gateway/distribution (channels, keys, billing) |
| [QuantumNous/new-api](https://github.com/QuantumNous/new-api) | Go | Active fork of one-api with heavier caching and channel tooling |
| [BerriAI/litellm](https://github.com/BerriAI/litellm) | Python | Proxy + multi-deployment router (cooldowns, fallbacks, strategies) |
| [maximhq/bifrost](https://github.com/maximhq/bifrost) | Go | High-performance LLM gateway (plugin core, governance, semantic cache) |
| [portkey-ai/gateway](https://github.com/portkey-ai/gateway) | TypeScript | AI gateway built on a middleware/target-tree config model |
| [caddyserver/caddy](https://github.com/caddyserver/caddy) | Go | Reverse proxy (health checks, retry, load balancing) |
| [traefik/traefik](https://github.com/traefik/traefik) + [vulcand/oxy](https://github.com/vulcand/oxy) | Go | Reverse proxy (retry body replay, expression circuit breaker, rate limit) |

Files cited as `repo:path` were read at
`raw.githubusercontent.com/<repo>/<default-branch>/<path>`.

## Applied in this round

### 1. Retry-After is honored over computed backoff — applied

- Source: `BerriAI/litellm:litellm/router.py`
  (`_time_to_sleep_before_retry` prefers the upstream's `Retry-After`,
  clamped, before falling back to exponential backoff) and
  `caddyserver/caddy:modules/caddyhttp/reverseproxy/reverseproxy.go`
  (retry loop re-selects a host per attempt).
- The upstream knows its own recovery schedule. A 429 carrying
  `Retry-After` is qualitatively different from a blind 5xx.
- Breakwater: `retry.StatusError` now carries the parsed and capped
  hint (`retry.ParseRetryAfter`: seconds, fractional seconds or
  HTTP-date, capped at 60s); the attempt loop prefers
  `DefaultClassifier.DelayHint(err)` over its jittered backoff.

### 2. A failover must not wait for the failed upstream's hint — applied

- Source: `litellm:litellm/router.py` (`_async_get_healthy_deployments`
  then `return 0` — with another healthy deployment available the next
  attempt fires immediately; only a drained group backs off).
- Breakwater: the relay strips the hint when the next attempt targets
  a different candidate (`retry.StripRetryAfter`), so the wait applies
  exactly to same-upstream retries.

### 3. Fatal upstream conditions disable the upstream, with a reason — applied

- Source: `songquanpeng/one-api:monitor/manage.go`
  (`ShouldDisableChannel`: 401, `insufficient_quota`,
  `authentication_error`, `invalid_api_key` → disable immediately) and
  `QuantumNous/new-api:service/relay_error.go` (structured,
  reason-carrying retry decisions).
- Transient faults belong to retry and breakers; deterministic faults
  (dead credentials, empty quota) deserve immediate ejection — and
  OpenAI reports quota exhaustion as a 429, so the status alone cannot
  tell it apart from a rate limit.
- Breakwater: the relay classifies completed error exchanges
  (`relay/fatal.go`: `insufficient_quota` envelope on any status, 401,
  403 with a provider-API envelope) and reports through
  `WithUpstreamFatalHook`; assembly wires it to the routing switch and
  `breakwater_upstream_auto_disabled_total`. The rules stay narrow on
  purpose — an auto disable takes real traffic down with it.

### 4. Manual and automatic disables are different states — applied

- Source: `one-api:model/channel.go` (`ChannelStatusManuallyDisabled`
  vs `ChannelStatusAutoDisabled`) and `new-api:service/channel.go`
  (auto-recovery checks `status != ChannelStatusAutoDisabled` → only
  auto-disabled channels ever come back on their own).
- A system that silently re-enables what an operator disabled is
  untrustworthy; a system that cannot distinguish the two flaps
  forever.
- Breakwater: `router.Switch` now keeps two disable channels. An
  auto disable records reason + moment and is lifted only by recovery;
  an operator enable clears both (human intent wins), and an operator
  disable subsumes any auto record. `GET /admin/routing` exposes the
  auto disables with their reasons.

### 5. Active recovery probing of ejected backends — applied

- Source: `caddyserver/caddy:modules/caddyhttp/reverseproxy/healthchecks.go`
  (`checkerLoop` ticker, per-upstream probe goroutines, shared client
  timeout); `traefik/traefik:pkg/healthcheck/healthcheck.go`
  (`Launch` runs **two** loops — unhealthy targets are checked on a
  faster interval than healthy ones); `one-api:service/channel.go`
  (periodic channel test re-enables auto-disabled channels).
- Passive detection alone means an idle gateway never learns an
  upstream has recovered.
- Breakwater: a recovery loop (`cmd/breakwater/recovery.go`) probes
  auto-disabled upstreams and — via the new `circuit.ActiveProbe`
  helper — drives a synthetic probe **through the breaker's own
  machine**: allowed exactly when a real request would be, reported
  like a served request, so a healthy answer closes the circuit
  without bending half-open semantics. Configured by
  `BREAKWATER_PROBE_INTERVAL` / `BREAKWATER_PROBE_TIMEOUT`; only
  upstreams with a `probe_url` participate.

### 6. `/v1/models` discovery — applied

- Source: `one-api` serves the OpenAI model-list endpoint from the
  channel model table; every OpenAI-compatible client (including
  agent CLIs) probes it on connect.
- Breakwater: `GET /v1/models` lists the client-facing names in the
  OpenAI list form — unauthenticated, no tenant data, rendered once at
  construction.

## Already aligned (confirmed against peers, no change needed)

### 7. Cooldown admission is a whitelist, not "any failure"

- Source: `litellm:litellm/router_utils/cooldown_handlers.py`
  (`_is_cooldown_required`: 429/401/408/404 and 5xx cool down; all
  other 4xx are the request's fault and must not demote the
  deployment).
- Breakwater: identical shape — 4xx (non-429) exchanges are
  `OutcomeClientFault` in breaker accounting and terminal in the
  classifier; only server faults advance the failure count.

### 8. Nothing retries after the first byte reaches the client

- Source: `litellm:litellm/router.py` (`FallbackAwareStreamWrapper`,
  `_anthropic_stream_should_decline_fallback`: `has_generated_content`
  refuses fallback); `traefik:pkg/middlewares/retry/retry.go`
  (bails out the moment `retryResponseWriter.written` is true);
  `caddyserver/caddy:.../reverseproxy.go` (`roundtripSucceededError`
  ends the retry loop once a response started).
- Breakwater: the same boundary exists as invariant I6 — committed
  streams terminate honestly through the error-event contract and are
  never replayed. Three independent projects converging on the same
  rule is a strong signal it is the correct one.

### 9. Gateway-originated errors do not cascade

- Source: `portkey-ai/gateway:src/handlers/handlerUtils.ts`
  (`gateway-exception` marked responses do not trigger fallback);
  `maximhq/bifrost:core/schemas/plugin.go` (`BifrostError.AllowFallbacks`).
- Breakwater: only transport failures, 5xx and 429 are retryable; the
  gateway's own conditions (`circuit_open`, `budget_exhausted`) are
  rendered once and never loop.

## Roadmap (learned, not yet applied)

### 10. Near-tie exploration in latency routing

`litellm:litellm/router_strategy/lowest_latency.py` sorts by score and
picks randomly among deployments within a buffer of the best —
spreading load across near-equals instead of pinning one. Breakwater's
static tie-break (configuration order) is deterministic and simpler;
for a single-user local gateway, determinism currently wins. Revisit
if several upstreams sit within a few milliseconds of each other.

### 11. Fallback chains across model groups

`litellm:litellm/router_utils/fallback_event_handlers.py`
(`run_async_fallback`: depth cap, per-request attempted set, budget
re-check per hop) and `portkey-ai/gateway:src/handlers/handlerUtils.ts`
(`tryTargetsRecursively`: fallback and load-balance targets nest
freely, `onStatusCodes` gates each hop). Breakwater's failover is
flat (one candidate list per request); a configured alias→alias
fallback chain with a depth cap is the natural next step.

### 12. Context-window pre-filtering

`litellm:litellm/router.py` (`_pre_call_check`: estimate input tokens
once, drop deployments whose `max_input_tokens` cannot fit, fail-open
on estimator errors). Saves a doomed round trip and gives the client a
clear error instead of a provider 400.

### 13. Ratio-based breaker expressions

`traefik:pkg/middlewares/circuitbreaker/circuit_breaker.go` +
`vulcand/oxy:cbreaker/cbreaker.go` (`ResponseCodeRatio`,
`NetworkErrorRatio`, `LatencyAtQuantileMS`; trip resets the metrics
window). Complementary to consecutive-failure counting: catches
chronic degradation (high error *ratio*, never a long streak).

### 14. Anti-flap thresholds on recovery

`caddyserver/caddy:.../healthchecks.go` marks a backend healthy only
after N consecutive successful active checks (`Passes`), not one.
Breakwater currently restores after a single healthy probe; the probe
interval bounds the blast radius of a flapping upstream, but a
consecutive-passes threshold would be strictly safer.

### 15. Session affinity

`new-api:service/channel_select.go` pins a session to a channel on the
first attempt only (retries must bypass it). For LLM workloads
affinity reuses upstream prompt caches; breakwater's CLI traffic would
benefit, but it interacts with the latency strategy and needs a
session key — postponed.

## Deliberately not copied

- **litellm's 14k-line `router.py`** — retry, fallback, caching and
  provider hacks in one object. Breakwater keeps `Selector` /
  switch / breaker / retry loop as separate small ports.
- **Redis-backed cooldown/limit state for a single process**
  (`litellm` dual-cache correction logic, one-api's `ORDER BY RANDOM()`
  DB path) — in-memory maps with the documented restart semantics are
  the right trade at this scale.
- **Keyword-list auto-ban** (one-api's message-substring matching,
  new-api's Aho-Corasick table) — breakwater's upstreams are
  explicitly configured; exact classification beats keyword guessing.
- **Feature breadth as such** (new-api's OAuth/task/midjourney
  surface, bifrost's multi-tenant governance hierarchy, portkey's
  `any`-typed recursive config tree) — the local gateway scenario
  needs none of it; the *shapes* (typed errors, one decision funnel,
  explicit state maps) were the transferable parts.
- **Email/webhook operator notifications** (one-api monitor) — the
  admin API, metrics and logs already surface every state change this
  study introduced.
