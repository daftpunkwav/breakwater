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

// Resolve implements Store.
func (s *PG) Resolve(ctx context.Context, apiKey string) (Tenant, error) {
	rows := s.pool.QueryRow(ctx, `
		SELECT t.id, t.name, tr.id, tr.rpm, tr.tpm,
		       tr.per_request_max_tokens, tr.monthly_quota, tr.allowed_models
		FROM api_keys k
		JOIN tenants t  ON t.id = k.tenant_id
		JOIN tiers  tr  ON tr.id = t.tier_id
		WHERE k.key_hash = $1 AND k.status = 'active'`,
		hashKey(apiKey))

	var tenant Tenant
	var tier Tier
	err := rows.Scan(&tenant.ID, &tenant.Name, &tier.ID, &tier.RPM, &tier.TPM,
		&tier.MaxTokens, &tier.MonthlyQuota, &tier.AllowedModels)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return Tenant{}, ErrUnauthorized
		}
		return Tenant{}, fmt.Errorf("auth: resolve key: %w", err)
	}
	tenant.Tier = tier
	return tenant, nil
}
