-- Local development seed: one tier, two tenants, two API keys.
-- The key_hash column holds SHA-256 of the raw key. The raw values are
-- the loadtest scenarios' default API_KEYs: 'bw-local-t1' (local-1)
-- and 'bw-local-t2' (local-2). The README quick start uses a separate
-- BREAKWATER_IDENTITY key and does not need this seed.
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

INSERT INTO api_keys (id, tenant_id, key_hash)
VALUES ('key-local-1', 'local-1',
        '8480a628527425db68d2d00ddb40662af3e08f1f70e986f184b985ebabb94834'),
       ('key-local-2', 'local-2',
        '77b067bf9a831837114736fca7c5eacf293138e63fffcc865d848d0ddfe2891b')
ON CONFLICT (id) DO NOTHING;
