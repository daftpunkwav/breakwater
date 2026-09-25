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

// Tier is the entitlement snapshot a request resolves with: the limits
// the governance layers enforce for this identity. The snapshot rides
// with the identity so steady-state resolution stays cache-local; the
// enforcement itself (buckets, leases) belongs to the limiter and
// quota modules, not here.
//
// A store returns the MERGED snapshot: the tier template with any
// user-level and key-level overrides already applied (MergeTier). The
// governance layers see one coherent policy and never re-merge.
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
	// DeniedModels removes models regardless of any allow list — the
	// operator's kill switch. Deny wins over allow, "*" denies all.
	DeniedModels []string
	// ModelQuotas caps the monthly token spend per model, overriding
	// the shared pool for those models; absent entries draw from the
	// pool. The quota layer consults it; this snapshot only carries it.
	ModelQuotas map[string]int64
	// Concurrency caps the in-flight requests of one identity; zero
	// disables the ceiling.
	Concurrency int64
}

// Role distinguishes what an identity may do on the management surface.
// gateway role governs administration, not inference: both roles call
// models under the same governance.
type Role string

const (
	RoleUser  Role = "user"
	RoleAdmin Role = "admin"
)

// Tenant is the identity a request is attributed to: a user of the
// gateway (Role carries the management-surface privilege) resolving
// through one API key.
type Tenant struct {
	ID   string
	Name string
	Role Role
	Tier Tier
}

// wildcardModel matches every model in an AllowedModels list.
const wildcardModel = "*"

// AllowsModel reports whether the identity may call the model. The
// deny list wins over everything: "*" denies all, a named entry
// removes that model even from an allow list. Otherwise the allow
// list decides — "*" admits everything, an empty list admits nothing
// (the fail-closed semantics the tier contract and schema promise).
func (t Tier) AllowsModel(model string) bool {
	for _, denied := range t.DeniedModels {
		if denied == wildcardModel || denied == model {
			return false
		}
	}
	for _, allowed := range t.AllowedModels {
		if allowed == wildcardModel || allowed == model {
			return true
		}
	}
	return false
}

// ErrUnauthorized reports an unknown, malformed or revoked API key.
var ErrUnauthorized = errors.New("auth: unknown or revoked api key")

// Store resolves API keys to tenants.
// Key revocation takes effect only after the implementation's cache TTL
// expires; that latency is part of the public contract.
type Store interface {
	Resolve(ctx context.Context, apiKey string) (Tenant, error)
}
