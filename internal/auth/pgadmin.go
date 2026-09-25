/**
 * @file pgadmin
 * @description The PostgreSQL implementation of the identity
 * administration port.
 *
 * Responsibilities:
 * - Execute the user/key lifecycle and the override-layer writes
 * against the identity schema
 * - Nothing else: authorization belongs to the admin handler; the
 * merged snapshot the pipeline sees is built by the resolution side
 */
package auth

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Identifier prefixes keep generated user ids and key ids in distinct
// namespaces.
const (
	userIDPrefix = "u_"
	keyIDPrefix  = "k_"
)

// randomID mints a prefixed identifier.
func randomID(prefix string) (string, error) {
	buf := make([]byte, 12)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("auth: generate id: %w", err)
	}
	return prefix + hexEncode(buf), nil
}

func hexEncode(b []byte) string {
	out := make([]byte, len(b)*2)
	const hexDigits = "0123456789abcdef"
	for i, v := range b {
		out[i*2] = hexDigits[v>>4]
		out[i*2+1] = hexDigits[v&0x0f]
	}
	return string(out)
}

// CreateUser implements AdminStore.
func (s *PG) CreateUser(ctx context.Context, name string, role Role, tierID string) (string, error) {
	if role != RoleUser && role != RoleAdmin {
		return "", fmt.Errorf("auth: unknown role %q", role)
	}
	id, err := randomID(userIDPrefix)
	if err != nil {
		return "", err
	}
	_, err = s.pool.Exec(ctx,
		`INSERT INTO tenants (id, name, role, tier_id) VALUES ($1, $2, $3, $4)`,
		id, name, role, tierID)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23503" {
		return "", fmt.Errorf("%w: %s", ErrUnknownTier, tierID)
	}
	if err != nil {
		return "", fmt.Errorf("auth: create user: %w", err)
	}
	return id, nil
}

// Users implements AdminStore.
func (s *PG) Users(ctx context.Context) ([]UserView, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT t.id, t.name, t.role, t.tier_id, t.overrides, t.created_at
		FROM tenants t ORDER BY t.created_at, t.id`)
	if err != nil {
		return nil, fmt.Errorf("auth: list users: %w", err)
	}
	defer rows.Close()

	var out []UserView
	for rows.Next() {
		var v UserView
		var raw []byte
		if err := rows.Scan(&v.ID, &v.Name, &v.Role, &v.Tier, &raw, &v.CreatedAt); err != nil {
			return nil, fmt.Errorf("auth: scan user: %w", err)
		}
		if v.Overrides, err = ParseOverride(raw); err != nil {
			return nil, fmt.Errorf("auth: user %s overrides: %w", v.ID, err)
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// SetUserLimits implements AdminStore.
func (s *PG) SetUserLimits(ctx context.Context, userID string, o LimitOverride) error {
	raw, err := json.Marshal(o)
	if err != nil {
		return fmt.Errorf("auth: encode overrides: %w", err)
	}
	tag, err := s.pool.Exec(ctx,
		`UPDATE tenants SET overrides = $1 WHERE id = $2`, raw, userID)
	if err != nil {
		return fmt.Errorf("auth: set user limits: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: %s", ErrUnknownUser, userID)
	}
	return nil
}

// CreateKey implements AdminStore. The cap check and the insert run in
// one transaction holding the tenant row lock: two concurrent CreateKey
// calls for one user serialize, so the MaxKeysPerUser ceiling cannot be
// raced past the way a plain check-then-insert allows.
func (s *PG) CreateKey(ctx context.Context, userID, name string) (IssuedKey, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return IssuedKey{}, fmt.Errorf("auth: begin create key: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	return createKeyTx(ctx, tx, userID, name)
}

// createKeyTx is CreateKey's transaction body, split so the failure
// paths a live database only reaches under fault conditions stay
// testable against a scripted pgx.Tx.
func createKeyTx(ctx context.Context, tx pgx.Tx, userID, name string) (IssuedKey, error) {
	// Lock the tenant's row for the check-then-insert span. A missing
	// row is the unknown-user sentinel — the same answer the foreign
	// key would give, one query earlier.
	var locked string
	if err := tx.QueryRow(ctx,
		`SELECT id FROM tenants WHERE id = $1 FOR UPDATE`, userID).Scan(&locked); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return IssuedKey{}, fmt.Errorf("%w: %s", ErrUnknownUser, userID)
		}
		return IssuedKey{}, fmt.Errorf("auth: lock user: %w", err)
	}

	var count int64
	if err := tx.QueryRow(ctx,
		`SELECT count(*) FROM api_keys WHERE tenant_id = $1`, userID).Scan(&count); err != nil {
		return IssuedKey{}, fmt.Errorf("auth: count keys: %w", err)
	}
	if count >= MaxKeysPerUser {
		return IssuedKey{}, fmt.Errorf("%w: user %s holds %d of %d",
			ErrTooManyKeys, userID, count, MaxKeysPerUser)
	}

	raw, err := GenerateKey()
	if err != nil {
		return IssuedKey{}, err
	}
	id, err := randomID(keyIDPrefix)
	if err != nil {
		return IssuedKey{}, err
	}
	var createdAt time.Time
	err = tx.QueryRow(ctx,
		`INSERT INTO api_keys (id, tenant_id, key_hash, name) VALUES ($1, $2, $3, $4)
		 RETURNING created_at`,
		id, userID, hashKey(raw), name).Scan(&createdAt)
	if err == nil {
		err = tx.Commit(ctx)
	}
	if err != nil {
		return IssuedKey{}, fmt.Errorf("auth: create key: %w", err)
	}
	return IssuedKey{
		KeyView: KeyView{ID: id, Name: name, Active: true, CreatedAt: createdAt},
		Raw:     raw,
	}, nil
}

// Keys implements AdminStore.
func (s *PG) Keys(ctx context.Context, userID string) ([]KeyView, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, name, status, overrides, created_at
		FROM api_keys WHERE tenant_id = $1 ORDER BY created_at, id`, userID)
	if err != nil {
		return nil, fmt.Errorf("auth: list keys: %w", err)
	}
	defer rows.Close()

	var out []KeyView
	for rows.Next() {
		var v KeyView
		var status string
		var raw []byte
		if err := rows.Scan(&v.ID, &v.Name, &status, &raw, &v.CreatedAt); err != nil {
			return nil, fmt.Errorf("auth: scan key: %w", err)
		}
		v.Active = status == "active"
		if v.Overrides, err = ParseOverride(raw); err != nil {
			return nil, fmt.Errorf("auth: key %s overrides: %w", v.ID, err)
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// SetKeyLimits implements AdminStore.
func (s *PG) SetKeyLimits(ctx context.Context, keyID string, o LimitOverride) error {
	raw, err := json.Marshal(o)
	if err != nil {
		return fmt.Errorf("auth: encode overrides: %w", err)
	}
	tag, err := s.pool.Exec(ctx,
		`UPDATE api_keys SET overrides = $1 WHERE id = $2`, raw, keyID)
	if err != nil {
		return fmt.Errorf("auth: set key limits: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: %s", ErrUnknownKey, keyID)
	}
	return nil
}

// SetKeyStatus implements AdminStore.
func (s *PG) SetKeyStatus(ctx context.Context, keyID string, active bool) error {
	status := "disabled"
	if active {
		status = "active"
	}
	tag, err := s.pool.Exec(ctx,
		`UPDATE api_keys SET status = $1 WHERE id = $2`, status, keyID)
	if err != nil {
		return fmt.Errorf("auth: set key status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: %s", ErrUnknownKey, keyID)
	}
	return nil
}

var _ AdminStore = (*PG)(nil)
