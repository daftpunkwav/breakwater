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
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
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
	ID            string   `json:"id"`
	RPM           int64    `json:"rpm"`
	TPM           int64    `json:"tpm"`
	MaxTokens     int64    `json:"max_tokens"`
	MonthlyQuota  int64    `json:"monthly_quota"`
	AllowedModels []string `json:"allowed_models"`
}

// StaticTenant is one tenant entry with its raw keys.
type StaticTenant struct {
	ID   string   `json:"id"`
	Name string   `json:"name"`
	Tier string   `json:"tier"`
	Keys []string `json:"keys"`
}

// Static resolves keys against the configured identity set. It is
// immutable after construction and safe for concurrent use.
type Static struct {
	byKey map[string]Tenant
}

// ParseStaticConfig decodes the raw identity JSON (the config layer
// keeps no knowledge of its shape).
func ParseStaticConfig(raw []byte) (StaticConfig, error) {
	var cfg StaticConfig
	if err := json.Unmarshal(raw, &cfg); err != nil {
		return StaticConfig{}, fmt.Errorf("auth: parse identity: %w", err)
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
		tiers[t.ID] = Tier{
			ID:            t.ID,
			RPM:           t.RPM,
			TPM:           t.TPM,
			MaxTokens:     t.MaxTokens,
			MonthlyQuota:  t.MonthlyQuota,
			AllowedModels: t.AllowedModels,
		}
	}

	s := &Static{byKey: make(map[string]Tenant, len(cfg.Tenants)*2)}
	for _, tn := range cfg.Tenants {
		if tn.ID == "" {
			return nil, fmt.Errorf("auth: static tenant without id")
		}
		tier, ok := tiers[tn.Tier]
		if !ok {
			return nil, fmt.Errorf("auth: tenant %s references unknown tier %q", tn.ID, tn.Tier)
		}
		tenant := Tenant{ID: tn.ID, Name: tn.Name, Tier: tier}
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
