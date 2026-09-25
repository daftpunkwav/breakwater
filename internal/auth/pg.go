/**
 * @file pg
 * @description The PostgreSQL-backed identity store: the system of
 * record for tiers, tenants and API keys.
 *
 * Responsibilities:
 * - Resolve a raw API key to its tenant and tier snapshot
 * - Nothing else: caching belongs to the LRU decorator, enforcement to
 *   the governance layers
 *
 * Keys are stored and looked up hashed, so the database never holds a
 * usable credential. Revocation is a status flip whose effect surfaces
 * within the auth cache TTL — the documented public latency.
 */
package auth

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PG resolves identities from the tenants database. It is safe for
// concurrent use.
type PG struct {
	pool *pgxpool.Pool
}

// NewPG connects the pool; connection failures surface at assembly.
func NewPG(ctx context.Context, dsn string) (*PG, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("auth: connect identity database: %w", err)
	}
	return &PG{pool: pool}, nil
}

// Ping reports database health for readiness probes.
func (s *PG) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// Close releases the pool; shutdown path only.
func (s *PG) Close() {
	s.pool.Close()
}

// Resolve implements Store: the tier template folded with the
// tenant's and then the key's overrides into the one effective
// snapshot the governance layers enforce.
func (s *PG) Resolve(ctx context.Context, apiKey string) (Tenant, error) {
	rows := s.pool.QueryRow(ctx, `
		SELECT t.id, t.name, t.role,
		       tr.id, tr.rpm, tr.tpm,
		       tr.per_request_max_tokens, tr.monthly_quota, tr.allowed_models,
		       t.overrides, k.overrides
		FROM api_keys k
		JOIN tenants t  ON t.id = k.tenant_id
		JOIN tiers  tr  ON tr.id = t.tier_id
		WHERE k.key_hash = $1 AND k.status = 'active'`,
		hashKey(apiKey))
	return resolveTenantRow(rows)
}

// rowScanner is the subset of a single-row query result the identity
// mapping needs; pgx.Row satisfies it.
type rowScanner interface {
	Scan(dest ...any) error
}

// resolveTenantRow maps one identity query row — the key's tenant,
// its tier template and both override layers — to the merged tenant
// snapshot. ErrNoRows maps to the definitive ErrUnauthorized (an
// unknown or revoked key), any other scan failure is wrapped as a
// transient resolution error. A broken overrides document is a data
// corruption error, not a silent skip: it fails the resolution.
func resolveTenantRow(row rowScanner) (Tenant, error) {
	var tenant Tenant
	var tier Tier
	var role Role
	var userRaw, keyRaw []byte
	err := row.Scan(&tenant.ID, &tenant.Name, &role, &tier.ID, &tier.RPM, &tier.TPM,
		&tier.MaxTokens, &tier.MonthlyQuota, &tier.AllowedModels, &userRaw, &keyRaw)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Tenant{}, ErrUnauthorized
		}
		return Tenant{}, fmt.Errorf("auth: resolve key: %w", err)
	}
	tenant.Role = role
	userOverride, err := ParseOverride(userRaw)
	if err != nil {
		return Tenant{}, fmt.Errorf("auth: tenant %s overrides: %w", tenant.ID, err)
	}
	keyOverride, err := ParseOverride(keyRaw)
	if err != nil {
		return Tenant{}, fmt.Errorf("auth: key overrides: %w", err)
	}
	tenant.Tier = MergeTier(tier, userOverride, keyOverride)
	return tenant, nil
}
