/**
 * @file admin
 * @description The identity administration port: user and API-key
 * lifecycle plus the layered limit overrides.
 *
 * Responsibilities:
 * - Define what the management surface needs from the system of
 *   record: create users, issue keys (bounded per user), set the
 *   user-level and key-level limit overrides, disable keys
 * - Nothing else: the admin handler owns HTTP shape and
 *   authorization; the governance layers enforce whatever the stores
 *   resolve
 *
 * Issued keys are returned exactly once, in the CreateKey response —
 * the store persists only the hash, mirroring the resolution side.
 * Override changes surface to live traffic within the auth cache TTL,
 * the documented revocation latency.
 */
package auth

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

// MaxKeysPerUser bounds the keys one user may hold. The limit keeps
// the administration surface (and the blast radius of one leaked
// device) finite.
const MaxKeysPerUser = 5

var (
	// ErrUnknownUser reports management against a nonexistent user.
	ErrUnknownUser = errors.New("auth: unknown user")
	// ErrUnknownKey reports management against a nonexistent key.
	ErrUnknownKey = errors.New("auth: unknown key")
	// ErrUnknownTier reports a user created against a nonexistent tier.
	ErrUnknownTier = errors.New("auth: unknown tier")
	// ErrTooManyKeys reports a key issuance beyond MaxKeysPerUser.
	ErrTooManyKeys = errors.New("auth: key limit reached")
)

// UserView is the management-surface projection of one user.
type UserView struct {
	ID        string        `json:"id"`
	Name      string        `json:"name"`
	Role      Role          `json:"role"`
	Tier      string        `json:"tier"`
	Overrides LimitOverride `json:"overrides"`
	CreatedAt time.Time     `json:"created_at"`
}

// KeyView is the management-surface projection of one API key. The
// raw key never appears here — it exists only in the CreateKey
// response.
type KeyView struct {
	ID        string        `json:"id"`
	Name      string        `json:"name"`
	Active    bool          `json:"active"`
	Overrides LimitOverride `json:"overrides"`
	CreatedAt time.Time     `json:"created_at"`
}

// IssuedKey is a freshly created key: the one time the raw secret is
// visible, plus the projection the listing will show from now on.
type IssuedKey struct {
	KeyView
	// Raw is the bearer secret, shown once at issuance.
	Raw string `json:"key"`
}

// AdminStore is the identity administration port. The PostgreSQL
// store implements it; the static identity set does not (the static
// mode is configuration, not an administration surface).
type AdminStore interface {
	// CreateUser adds a user; the generated id is returned.
	CreateUser(ctx context.Context, name string, role Role, tierID string) (string, error)
	// Users lists every user in stable order.
	Users(ctx context.Context) ([]UserView, error)
	// SetUserLimits replaces the user-level override layer.
	SetUserLimits(ctx context.Context, userID string, o LimitOverride) error
	// CreateKey issues a key to the user; MaxKeysPerUser is enforced.
	CreateKey(ctx context.Context, userID, name string) (IssuedKey, error)
	// Keys lists the user's keys.
	Keys(ctx context.Context, userID string) ([]KeyView, error)
	// SetKeyLimits replaces one key's override layer.
	SetKeyLimits(ctx context.Context, keyID string, o LimitOverride) error
	// SetKeyStatus enables or disables one key.
	SetKeyStatus(ctx context.Context, keyID string, active bool) error
}

// GenerateKey mints a fresh API key: a bw- prefixed random secret.
// Callers persist only hashKey(raw).
func GenerateKey() (string, error) {
	buf := make([]byte, 24)
	if _, err := rand.Read(buf); err != nil {
		return "", fmt.Errorf("auth: generate key: %w", err)
	}
	return "bw-" + hex.EncodeToString(buf), nil
}
