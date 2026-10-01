# cmd/breakwater/ agent rules

Shared rules: [../AGENTS.md](../AGENTS.md). Assembly map:
[README.md](README.md). Env table: [../../README.md](../../README.md).

## Process boundary

- Configuration arrives only through `BREAKWATER_*` via `internal/config`.
  This binary has no CLI flags.
- `main` calls `run`. `run` calls `config.Load`, then `serve`. `serve`
  returns errors and does not exit the process.
- Behavior stays in `internal/*`. This directory assembles the process
  and owns signals, exit codes, and the root logger.

## Assembly order

Build in this order: metrics registry; access log and insights store,
fanned into one sink; governance backends; breaker registry or
`circuit.NopBreaker`; identity store; governance middleware and its
workers; upstream bindings and the router; relay; one `pipeline.Chain`
per client format; admin handlers; HTTP server; background workers.

Per-format chain, outermost first: carrier, request id, format,
governance, `RecoveryStage`. `RecoveryStage` stays inside
`ObservationStage`.

Governance order: observation, auth, model authorization, concurrency,
rate limit, quota, cache when caching is enabled. With no identity
store, governance is observation only.

## Startup

- A configured Redis that fails Ping fails startup.
- Fallback chains and context ceilings that name an unknown model fail
  startup (`validateModelNames`).
- With no identity configured, log a warning and skip auth, model
  authorization, concurrency, rate limit, quota, and cache.
  Observation still runs.
- Wrap the identity store in `auth.NewCachedStore`: 60s positive TTL,
  5s negative TTL.
- An empty `BREAKWATER_REDIS_NAMESPACE` on a configured Redis logs a
  startup warning.

## Shutdown

- A server drain past the grace window is a warned clean stop.
- Drain the access log on the success path, the error path, and the
  drain-timeout path. The log drain window is `shutdownLogGrace` (5s).

## Workers

- `recovery.go` probes auto-disabled and breaker-ejected upstreams.
  Restore an auto-disabled upstream only after
  `BREAKWATER_PROBE_THRESHOLD` consecutive healthy probes.
  `BREAKWATER_PROBE_BACKOFF_MAX` spaces failed probes on a doubling
  ladder; a healthy probe clears the ladder.
- Start the reconciler only when `snapshotSource` is the Redis ledger,
  `identity.admin` is set, `BREAKWATER_POSTGRES_DSN` is set, and
  `BREAKWATER_RECONCILE_INTERVAL` is positive. `startReconciler`
  creates the snapshot store.
- When a static identity set is configured, seed each tenant once
  through `EnsureBalance`. Do not reset a balance that already exists.
  A PostgreSQL identity does not seed.
