# internal/auth/ agent rules

Shared rules: [../AGENTS.md](../AGENTS.md). Package map: [README.md](README.md).

- Store and look up API keys only as HMAC-SHA256 digests keyed by
  `BREAKWATER_KEY_PEPPER`, in PostgreSQL and in the static set. No raw
  secret is persisted. Keys issued through `CreateKey` appear once, in
  that response; static-identity keys reach the gateway in raw form
  through `BREAKWATER_IDENTITY`, whose environment or file the operator
  must protect, and live only in process memory after that.
- `ParseStaticConfig` rejects malformed JSON and unknown fields.
  Startup fails when that parse fails.
- `GenerateKey` uses the `bw-` prefix. `MaxKeysPerUser` is 5.
- Return the merged snapshot from `MergeTier`. Callers do not merge
  again.
- Scalar limits take the nearest set layer: key, then user, then tier.
- `denied_models` unions across layers. `allowed_models` intersects
  across layers.
- A corrupted overrides document fails resolution.
- Do not cache a transient backend failure. The positive TTL is the
  revocation latency: 60s at the composition root. The negative TTL
  there is 5s.
- `Role` does not gate inference or admin routes. Admin routes use
  `BREAKWATER_ADMIN_TOKEN`. Inference limits are the same for both
  roles.
- `Tenant.KeyID` is empty in static mode. Per-key attribution and
  per-key overrides require the PostgreSQL store.
- PostgreSQL tests require `BREAKWATER_TEST_POSTGRES_DSN`.
