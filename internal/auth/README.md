# auth/

> Language: **English** | [简体中文](README.zh.md)

Identity for the governance pipeline: API keys, tenants, roles, tiers and the
layered limit model. It owns key resolution (`Store`) and identity
administration (`AdminStore`); it does NOT own enforcement — RPM/TPM buckets
and the concurrency gate live in [`internal/limiter`](../limiter/), the
balance ledger in [`internal/quota`](../quota/). It is the first stage of the
inference chain: auth → model authorization → concurrency → limiter → quota →
cache.

Two stores implement `Store`: `Static` (the `BREAKWATER_IDENTITY` JSON, for
local development and evidence runs) and `PGStore` (PostgreSQL, the system of
record behind `BREAKWATER_POSTGRES_DSN`). The composition root wraps either
in the process-local `CachedStore` LRU, so steady-state resolution stays off
distributed I/O. Only `PGStore` implements `AdminStore` (the `/admin/users` and
`/admin/keys` surface); the static mode is configuration, not an
administration surface.

## Files

| File | Role |
| --- | --- |
| `auth.go` | Contracts: `Tenant`, `Tier`, `Store`, `ErrUnauthorized`, `AllowsModel` (deny wins over allow; an empty allow list admits nothing) |
| `limits.go` | `LimitOverride` and `MergeTier`: user-level and key-level override layers folded into the effective tier |
| `static.go` | `Static` store over the config JSON; keys hashed at build; `Tenants()`/`TenantByID()` feed balance seeding |
| `pg.go` | `PGStore`: one join over `api_keys`/`tenants`/`tiers` resolved by `key_hash`, `status = 'active'` only |
| `lru.go` | `CachedStore`: LRU + TTL decoration; caches positives and definitive `ErrUnauthorized` negatives, never transient failures |
| `admin.go` | `AdminStore` port, `MaxKeysPerUser` (5), `GenerateKey` (`bw-` prefix, raw secret shown once) |
| `pgadmin.go` | PostgreSQL `AdminStore`: user/key lifecycle, override writes, transactional key issuance |

## Tests

Unit tests cover merge semantics, the LRU and static identity; the pg and
pgadmin integration tests need PostgreSQL (`BREAKWATER_TEST_POSTGRES_DSN`).

## Invariants

- Keys are stored and looked up only as HMAC-SHA256 digests keyed by
  `BREAKWATER_KEY_PEPPER` (database and static set alike); no raw secret
  is persisted. Keys issued through `CreateKey` appear once, in that
  response; static-identity keys reach the gateway in raw form through
  `BREAKWATER_IDENTITY`, whose environment or file the operator must
  protect. Raw keys are otherwise held only in process memory,
  resolution caches included. Changing the pepper invalidates every
  persisted key_hash.
- Stores return the merged snapshot (`MergeTier`); the governance layers
  never re-merge. Scalars take the nearest set layer (key over user over
  tier); `denied_models` only unions; `allowed_models` only intersects. A
  corrupted overrides document fails resolution instead of being skipped.
- Revocation latency equals the cache positive TTL (60s at the composition
  root, negatives 5s) and is part of the public contract; transient backend
  failures are never cached.
- `Role` is an annotation, not an enforcement boundary: the admin surface is
  guarded by `BREAKWATER_ADMIN_TOKEN`, and inference is governed identically
  for both roles.
- `Tenant.KeyID` is empty in static mode; per-key attribution and per-key
  overrides are database-deployment features.
