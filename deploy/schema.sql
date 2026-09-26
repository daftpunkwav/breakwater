-- Initial identity and tiering schema for the breakwater gateway.
-- Covers tiers, tenants and API keys; quota ledger and reconciliation
-- DDL land together with the lease protocol implementation.
-- Schema changes are applied on first boot of a fresh database; a
-- migration mechanism joins with the auth milestone.

-- Tier is the unit of quota and permission management: tenants reference
-- a tier instead of carrying their own limits, so bulk administration
-- edits a handful of tiers rather than every tenant.
CREATE TABLE IF NOT EXISTS tiers (
    id             TEXT PRIMARY KEY,
    rpm            INTEGER NOT NULL,
    tpm            BIGINT NOT NULL,
    monthly_quota  BIGINT NOT NULL,
    -- Per-request token clamp of the TPM reservation: a declared
    -- max_tokens is capped here so oversized requests cannot monopolize
    -- a tenant bucket. Zero disables the clamp.
    per_request_max_tokens BIGINT NOT NULL DEFAULT 0,
    -- Empty array allows no model (fail-closed); list every allowed model
    -- explicitly.
    allowed_models TEXT[] NOT NULL DEFAULT '{}',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS tenants (
    id         TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    tier_id    TEXT NOT NULL REFERENCES tiers (id),
    -- Management-surface privilege: 'user' (default) or 'admin'. The
    -- role governs administration only; inference governance is the
    -- same for both.
    role       TEXT NOT NULL DEFAULT 'user',
    -- User-level limit deviations (the auth.LimitOverride JSON shape),
    -- applied to every key of this tenant on top of the tier defaults;
    -- the regular path is changing the tier.
    overrides  JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS api_keys (
    id         TEXT PRIMARY KEY,
    tenant_id  TEXT NOT NULL REFERENCES tenants (id),
    key_hash   TEXT NOT NULL UNIQUE,
    -- Operator-facing label; the raw key is shown once at creation.
    name       TEXT NOT NULL DEFAULT '',
    status     TEXT NOT NULL DEFAULT 'active',
    -- Key-level limit deviations (the same auth.LimitOverride JSON
    -- shape), applied on top of the tenant's overrides. Denied models
    -- union across layers; scalar limits take the nearest set value.
    overrides  JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_api_keys_tenant ON api_keys (tenant_id);

-- Ledger snapshots taken by the reconcile protocol (PRD Q6): every
-- interval the Redis hot ledger is appended here, and consecutive
-- snapshots must satisfy the balance identity (the balance falls only
-- by debits minus refunds; every balance movement pairs with exactly
-- one counter movement inside the ledger scripts). Consumed records
-- actual usage for observation and is not part of the identity. Epoch
-- skips the identity check across manual balance corrections.
CREATE TABLE IF NOT EXISTS quota_snapshots (
    id         BIGSERIAL PRIMARY KEY,
    tenant_id  TEXT NOT NULL,
    balance    BIGINT NOT NULL,
    consumed   BIGINT NOT NULL,
    refunded   BIGINT NOT NULL,
    debited    BIGINT NOT NULL DEFAULT 0,
    epoch      BIGINT NOT NULL DEFAULT 0,
    taken_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_quota_snapshots_tenant ON quota_snapshots (tenant_id, taken_at DESC);

-- The monitoring and assessment record (PRD observability): one row
-- per finished request, written asynchronously in batches. The
-- failure taxonomy lives in error_code: the governance rejection code
-- (missing_api_key, invalid_api_key, identity_unavailable,
-- model_not_allowed, concurrency_limit_exceeded, rate_limit_exceeded,
-- insufficient_quota, governance_unavailable, ...), the relay's
-- gateway/abort code (no_upstream, circuit_open, budget_exhausted,
-- upstream_unreachable, upstream_timeout, upstream_reset, ...) or
-- empty for upstream error passthroughs whose status code classifies
-- them (coarse upstream_4xx / upstream_5xx buckets in the report
-- queries). Client disconnects are status 499 and never count as
-- failures.
CREATE TABLE IF NOT EXISTS request_log (
    id          BIGSERIAL PRIMARY KEY,
    time        TIMESTAMPTZ NOT NULL,
    tenant_id   TEXT NOT NULL DEFAULT '',
    key_id      TEXT NOT NULL DEFAULT '',
    request_id  TEXT NOT NULL DEFAULT '',
    model       TEXT NOT NULL DEFAULT '',
    upstream    TEXT NOT NULL DEFAULT '',
    path        TEXT NOT NULL DEFAULT '',
    status      INTEGER NOT NULL,
    duration_ms BIGINT NOT NULL,
    tokens      BIGINT NOT NULL DEFAULT 0,
    cache_hit   BOOLEAN NOT NULL DEFAULT FALSE,
    streamed    BOOLEAN NOT NULL DEFAULT FALSE,
    error_code  TEXT NOT NULL DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_request_log_time ON request_log (time);
CREATE INDEX IF NOT EXISTS idx_request_log_tenant ON request_log (tenant_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_request_log_model ON request_log (model, time DESC);
