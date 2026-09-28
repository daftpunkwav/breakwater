/**
 * @file static
 * @description Identity set served from process memory: the system of
 * record substitute for local development and evidence runs.
 *
 * Responsibilities:
 * - Resolve API keys against a fixed, config-supplied identity set
 * - Nothing else: no persistence, no revocation latency beyond process
 *   lifetime; deployments with a real system of record wrap the pg
 *   store in the LRU cache instead
 *
 * Keys are stored hashed so a config leak does not leak usable keys,
 * mirroring the schema's key_hash discipline.
 */
package auth

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
)

// StaticConfig is the config-supplied identity set.
type StaticConfig struct {
	Tiers []StaticTier `json:"tiers"`
	// Tenants reference tiers by ID and carry their raw API keys; keys
	// are hashed before lookup structures are built.
	Tenants []StaticTenant `json:"tenants"`
}

// StaticTier is one tier entry.
type StaticTier struct {
	ID            string           `json:"id"`
	RPM           int64            `json:"rpm"`
	TPM           int64            `json:"tpm"`
	MaxTokens     int64            `json:"max_tokens"`
	MonthlyQuota  int64            `json:"monthly_quota"`
	AllowedModels []string         `json:"allowed_models"`
	DeniedModels  []string         `json:"denied_models"`
	ModelQuotas   map[string]int64 `json:"model_quotas"`
	Concurrency   int64            `json:"concurrency"`
}

// StaticTenant is one tenant entry with its raw keys.
type StaticTenant struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Tier string `json:"tier"`
	Role Role   `json:"role"`
	// Overrides are the user-level limit deviations applied to every
	// key of this tenant; omitted fields inherit the tier. The static
	// mode has no per-key overrides — key-granular administration is a
	// database deployment's feature.
	Overrides LimitOverride `json:"overrides"`
	Keys      []string      `json:"keys"`
}

// Static resolves keys against the configured identity set. It is
// immutable after construction and safe for concurrent use.
type Static struct {
	byKey map[string]Tenant
}

// validTenantID reports whether a tenant id stays inside the character
// set every consumer assumes: Redis key segments (the quota and limiter
// keys), access-log fields and admin URL path segments. The same rule
// governs upstream ids (internal/config); the PostgreSQL store generates
// ids itself, so this guards the operator-supplied static set.
var validTenantID = regexp.MustCompile(`^[A-Za-z0-9._-]{1,128}$`).MatchString

// ParseStaticConfig decodes the raw identity JSON (the config layer
// keeps no knowledge of its shape). The set's members are a fixed
// schema, so a typo'd member (say "kyes" for "keys") fails loudly
// instead of silently yielding an identity nobody can resolve.
func ParseStaticConfig(raw []byte) (StaticConfig, error) {
	var cfg StaticConfig
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&cfg); err != nil {
		return StaticConfig{}, fmt.Errorf("auth: parse identity: %w", err)
	}
	if err := dec.Decode(&cfg); !errors.Is(err, io.EOF) {
		return StaticConfig{}, fmt.Errorf("auth: parse identity: trailing data after the JSON value")
	}
	return cfg, nil
}

// NewStatic builds the store, rejecting unknown tier references and
// duplicate keys at assembly time.
func NewStatic(cfg StaticConfig) (*Static, error) {
	tiers := make(map[string]Tier, len(cfg.Tiers))
	for _, t := range cfg.Tiers {
		if t.ID == "" {
			return nil, fmt.Errorf("auth: static tier without id")
		}
		// StaticTier mirrors Tier field for field: the direct conversion
		// keeps the wire config honest about the identity snapshot it
		// feeds and fails to compile if the shapes drift apart.
		tiers[t.ID] = Tier(t)
	}

	s := &Static{byKey: make(map[string]Tenant, len(cfg.Tenants)*2)}
	for _, tn := range cfg.Tenants {
		if tn.ID == "" {
			return nil, fmt.Errorf("auth: static tenant without id")
		}
		if !validTenantID(tn.ID) {
			return nil, fmt.Errorf("auth: static tenant has an invalid id %q: ids are 1-128 characters of [A-Za-z0-9._-]", tn.ID)
		}
		tier, ok := tiers[tn.Tier]
		if !ok {
			return nil, fmt.Errorf("auth: tenant %s references unknown tier %q", tn.ID, tn.Tier)
		}
		if tn.Role == "" {
			tn.Role = RoleUser
		}
		if tn.Role != RoleUser && tn.Role != RoleAdmin {
			return nil, fmt.Errorf("auth: tenant %s has unknown role %q", tn.ID, tn.Role)
		}
		tenant := Tenant{
			ID:   tn.ID,
			Name: tn.Name,
			Role: tn.Role,
			Tier: MergeTier(tier, tn.Overrides),
		}
		tenant.Tier.ID = tier.ID
		for _, raw := range tn.Keys {
			if raw == "" {
				return nil, fmt.Errorf("auth: tenant %s has an empty api key", tn.ID)
			}
			hash := hashKey(raw)
			if _, dup := s.byKey[hash]; dup {
				return nil, fmt.Errorf("auth: duplicate api key for tenant %s", tn.ID)
			}
			s.byKey[hash] = tenant
		}
	}
	return s, nil
}

// Resolve implements Store.
func (s *Static) Resolve(_ context.Context, apiKey string) (Tenant, error) {
	tenant, ok := s.byKey[hashKey(apiKey)]
	if !ok {
		return Tenant{}, ErrUnauthorized
	}
	return tenant, nil
}

// Tenants lists the configured tenant IDs in stable order; the balance
// seeding and evidence tooling iterate over it.
func (s *Static) Tenants() []string {
	ids := make([]string, 0, len(s.byKey))
	seen := make(map[string]struct{}, len(s.byKey))
	for _, tn := range s.byKey {
		if _, ok := seen[tn.ID]; ok {
			continue
		}
		seen[tn.ID] = struct{}{}
		ids = append(ids, tn.ID)
	}
	sort.Strings(ids)
	return ids
}

// TenantByID returns one configured tenant.
func (s *Static) TenantByID(id string) (Tenant, bool) {
	for _, tn := range s.byKey {
		if tn.ID == id {
			return tn, true
		}
	}
	return Tenant{}, false
}

// hashKey derives the lookup hash of a raw API key.
func hashKey(raw string) string {
	sum := sha256.Sum256([]byte(raw))
	return hex.EncodeToString(sum[:])
}
