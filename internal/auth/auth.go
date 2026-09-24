/**
 * @file auth
 * @description API key authentication contracts.
 *
 * Responsibilities:
 * - Define the tenant identity and the key resolution port
 * - Nothing else: caching (in-process LRU, TTL ~60s) belongs to the
 *   implementation, and per-tenant limits belong to the limiter module
 *
 * The steady-state authentication path resolves from process memory; a
 * cache miss may read the system of record to backfill, keeping
 * distributed I/O off the hot path.
 */
package auth

import (
	"context"
	"errors"
)

// Tier is the entitlement snapshot a tenant resolves with: the limits
// the governance layers enforce for this identity. The snapshot rides
// with the identity so steady-state resolution stays cache-local; the
// enforcement itself (buckets, leases) belongs to the limiter and
// quota modules, not here.
type Tier struct {
	ID string
	// RPM and TPM are the per-minute request and token ceilings; zero
	// disables that ceiling.
	RPM int64
	TPM int64
	// MaxTokens is the per-request token clamp of the TPM reservation
	// (the self-inflicted-DoS guard); below one it is absent.
	MaxTokens int64
	// MonthlyQuota documents the tier's monthly token budget; the live
	// balance ledger is the quota module's domain.
	MonthlyQuota int64
	// AllowedModels lists the models the tier may call; empty allows
	// none (fail-closed).
	AllowedModels []string
}

// Tenant is the identity a request is attributed to.
type Tenant struct {
	ID   string
	Name string
	Tier Tier
}

// ErrUnauthorized reports an unknown, malformed or revoked API key.
var ErrUnauthorized = errors.New("auth: unknown or revoked api key")

// Store resolves API keys to tenants.
// Key revocation takes effect only after the implementation's cache TTL
// expires; that latency is part of the public contract.
type Store interface {
	Resolve(ctx context.Context, apiKey string) (Tenant, error)
}
