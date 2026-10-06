-- Local development seed: one tier, two tenants, two API keys.
-- The key_hash column holds HMAC-SHA256 of the raw key, keyed by
-- BREAKWATER_KEY_PEPPER. The digests below assume the pepper is unset
-- (the empty key) at seed time; a deployment that sets a pepper must
-- recompute them (TestSeedKeyHashMatchesHashKey pins the Go side
-- against this file). Reapplying the seed refreshes the two dev keys'
-- hashes, so a database seeded by an older build heals on reapply.
-- The raw values are the loadtest scenarios' default API_KEYs:
-- 'bw-local-t1' (local-1) and 'bw-local-t2' (local-2). The README
-- quick start uses a separate BREAKWATER_IDENTITY key and does not
-- need this seed.
-- docker-compose.yml mounts this file as 02-seed.sql so the first boot
-- applies it after the schema; it stays idempotent if applied manually:
--   docker compose -f deploy/docker-compose.yml exec -T postgres \
--     psql -U breakwater -d breakwater < deploy/seed.sql

INSERT INTO tiers (id, rpm, tpm, monthly_quota, per_request_max_tokens, allowed_models)
VALUES ('free', 60, 200000, 10000000, 4096, ARRAY['*'])
ON CONFLICT (id) DO NOTHING;

-- local-2 shows the user-level override layer: a model denied for
-- every key of the tenant plus a concurrency ceiling of 4.
INSERT INTO tenants (id, name, role, tier_id, overrides)
VALUES ('local-1', 'Local Tenant One', 'user', 'free', '{}'),
       ('local-2', 'Local Tenant Two', 'user', 'free',
        '{"denied_models":["secret-model"],"concurrency":4}')
ON CONFLICT (id) DO NOTHING;

-- Unlike the tiers and tenants above, a conflicting key row is restored
-- to the shape the seed owns (tenant and hash): these two rows are the
-- seed's own, and a stale hash or tenant from an older build would
-- otherwise keep the loadtest keys rejected or misattributed.
INSERT INTO api_keys (id, tenant_id, key_hash)
VALUES ('key-local-1', 'local-1',
        'dbd756faa78508795443155672311944720bab34224dcdde87e6a1bfb2838088'),
       ('key-local-2', 'local-2',
        'a544711afeb45d9c00135b1c18c3e05582f59bd78272137fd30fc204aed2fd76')
ON CONFLICT (id) DO UPDATE
SET tenant_id = EXCLUDED.tenant_id,
    key_hash  = EXCLUDED.key_hash;
