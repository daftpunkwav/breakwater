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

// Tenant is the identity a request is attributed to.
type Tenant struct {
	ID   string
	Name string
}

// ErrUnauthorized reports an unknown, malformed or revoked API key.
var ErrUnauthorized = errors.New("auth: unknown or revoked api key")

// Store resolves API keys to tenants.
// Key revocation takes effect only after the implementation's cache TTL
// expires; that latency is part of the public contract.
type Store interface {
	Resolve(ctx context.Context, apiKey string) (Tenant, error)
}
