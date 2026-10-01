# internal/config/ agent rules

Shared rules: [../AGENTS.md](../AGENTS.md). Package row:
[../README.md](../README.md). Env table: [../../README.md](../../README.md).

- This package is a leaf. Import no other project package.
- Declare every value on `Config`. Load `BREAKWATER_*` in `env.go`.
- An invalid value refuses to boot and names the variable.
- Unknown JSON members on the upstream table refuse to boot. Identity
  JSON stays a raw string in this package; `internal/auth` parses it.
- Upstream ids match `[A-Za-z0-9._-]{1,128}`. A base URL without an
  http or https scheme and host refuses to boot.
- Carry identity JSON as a raw string. The document schema belongs to
  `internal/auth`.
- Do not check fallback or context-ceiling names against the configured
  model set here. `cmd/breakwater` does that at assembly.
- Non-positive `BREAKWATER_SHUTDOWN_GRACE` refuses to boot.
- A quota lease TTL below the derived floor refuses to boot. With no
  overall deadline and no stream timeout to derive from, the lease TTL
  defaults to 24h.
- `guardDeploymentPosture` refuses these postures unless
  `BREAKWATER_ALLOW_UNAUTHENTICATED=1`: upstreams with no identity
  source; a PostgreSQL DSN with an empty admin token; armed
  configuration (upstreams, identity, or insights) with an empty admin
  token. A malformed boolean refuses to boot.
- A `file://` identity value with an empty or unreadable path refuses
  to boot.
- Zero entries in `BREAKWATER_CONTEXT_LIMITS` refuse to boot.
