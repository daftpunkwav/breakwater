# breakwater/

> Language: **English** | [简体中文](README.zh.md)

The gateway binary and its composition root: the one place that decides which backend serves each concern and in what order everything is wired. Behavior lives in the `internal/*` packages; this directory only bridges the process boundary (signals, exit codes, the root logger) and assembles. Configuration arrives exclusively through `BREAKWATER_*` environment variables via `internal/config` — there are no CLI flags.

## Files

| File | Role |
|---|---|
| `main.go` | The process boundary: root `slog` logger, the Makefile-injected `version`, `config.Load()`, then `serve`. Deliberately thin; `serve` returns errors instead of exiting, keeping the whole path testable |
| `serve.go` | The composition heart: assembly order, the per-format middleware chains, the run lifecycle (SIGINT/SIGTERM, shutdown ordering, drain-timeout handling) |
| `governance.go` | Backend selection: `newGovernanceBackends` (Redis with a fail-fast Ping, or in-memory), `newAuthStore` (PostgreSQL / static `BREAKWATER_IDENTITY` / none, each wrapped in `auth.NewCachedStore` with a 60 s positive / 5 s negative TTL), `seedBalances` |
| `obsassembly.go` | Observation assembly: the JSONL access log file sink (`BREAKWATER_ACCESS_LOG_PATH`), the insights store (`BREAKWATER_INSIGHTS_DSN`, defaulting to `BREAKWATER_POSTGRES_DSN`), the `combinedSink` fan-out |
| `adminassembly.go` | Admin surface assembly: `buildAdmin` binds the balance, breaker state/reset and on-demand probe endpoints to the live ledger, breaker registry and upstream adapters (`adminBindings`) |
| `recovery.go` | The active recovery loop: probes auto-disabled upstreams (`BREAKWATER_PROBE_THRESHOLD` consecutive healthy answers lift the disable; `BREAKWATER_PROBE_BACKOFF_MAX` spaces the probes of a failing one out on a doubling ladder) and drives synthetic probes through the breaker for breaker-ejected upstreams |

## Assembly order

`serve` builds, in order: the metrics registry → access log + insights store, fanned into one `combinedSink` → governance backends (limiter + quota ledger; a configured Redis must answer a Ping or startup fails) → the breaker registry (or `circuit.NopBreaker{}`) → the identity store (identity is what arms the governance pipeline: with none configured, the pipeline runs without governance stages and a warning is logged) → the governance middleware template (observation, auth, model authorization, concurrency, rate limit, quota, optional cache), plus the sweeper, reconciler and balance seeding workers it starts → upstream bindings and the router → the relay engine → one `pipeline.Chain` per client format (carrier → request id → format → governance → recovery) → admin handlers → the HTTP server → the background workers (drop-counter publishing, the recovery loop).

## Invariants

- Composition happens exclusively here (repo zoning rule): no wire-up decisions live in `internal/*`.
- A configured Redis that cannot answer is a deployment failure, not a degraded start — the limiter fail-closes on it. Redis without `BREAKWATER_REDIS_NAMESPACE` draws a startup warning (shared-instance hazard).
- The access log drains before the process exits — happy path, error path, and drain timeout alike; a server drain past its grace window is a warned clean stop, not a failure, and the access log gets a further 5 s drain window of its own (`shutdownLogGrace`).
- Fallback chains and context ceilings naming an unknown model refuse to boot (`validateModelNames`); a typo must never filter silently.

Env var reference: root [README](../../README.md). Local stack: [deploy/docker-compose.yml](../../deploy/docker-compose.yml); wiring real providers: [docs/DEPLOY-LOCAL.md](../../docs/DEPLOY-LOCAL.md).
