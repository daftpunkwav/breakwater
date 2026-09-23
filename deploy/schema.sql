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
    -- Empty array allows no model (fail-closed); list every allowed model
    -- explicitly.
    allowed_models TEXT[] NOT NULL DEFAULT '{}',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS tenants (
    id         TEXT PRIMARY KEY,
    name       TEXT NOT NULL,
    tier_id    TEXT NOT NULL REFERENCES tiers (id),
    -- Per-tenant deviation from the tier defaults, applied sparsely for
    -- individual cases; the regular path is changing the tier.
    overrides  JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS api_keys (
    id         TEXT PRIMARY KEY,
    tenant_id  TEXT NOT NULL REFERENCES tenants (id),
    key_hash   TEXT NOT NULL UNIQUE,
    status     TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_api_keys_tenant ON api_keys (tenant_id);
