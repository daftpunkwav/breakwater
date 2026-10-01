# internal/config/ agent rules

Shared rules: [../AGENTS.md](../AGENTS.md). Package row:
[../README.md](../README.md). Env table: [../../README.md](../../README.md).

- This package is a leaf. Import no other project package.
- Declare every value on `Config`. Load `BREAKWATER_*` in `env.go`.
- An invalid value refuses to boot and names the variable.
- Unknown JSON members on the upstream table refuse to boot. Identity
  JSON stays a raw string here. `internal/auth` owns the schema and
  parses it.
- Upstream ids match `[A-Za-z0-9._-]{1,128}`. A base URL without an
  http or https scheme and host refuses to boot.
- Do not check fallback or context-ceiling names against the configured
  model set here. `cmd/breakwater` does that at assembly.
- Non-positive `BREAKWATER_SHUTDOWN_GRACE` refuses to boot.
- A negative `BREAKWATER_QUOTA_LEASE_TTL` refuses to boot. Zero means
  derive. Derivation needs both overall deadline and stream timeout
  positive; the value is their sum plus `leaseTTLHeadroom` (1m). If
  either bound is absent, `LeaseTTL` is 24h. A configured TTL greater
  than zero and less than or equal to overall deadline plus stream
  timeout refuses to boot. That check runs only when both bounds are
  positive.
- `guardDeploymentPosture` refuses these postures unless
  `BREAKWATER_ALLOW_UNAUTHENTICATED=1`: upstreams with no identity
  source; a PostgreSQL DSN with an empty admin token; armed
  configuration (upstreams, identity, or insights) with an empty admin
  token. A malformed boolean refuses to boot.
- A `file://` identity value with an empty or unreadable path refuses
  to boot.
- An empty model name or a non-positive entry in
  `BREAKWATER_CONTEXT_LIMITS` refuses to boot.
