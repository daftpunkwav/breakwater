/**
 * @file limits
 * @description The layered limit model: a tier template, user-level
 * overrides, key-level overrides and the merge that folds them into
 * the one effective snapshot.
 *
 * Responsibilities:
 * - Define the override shape shared by the admin API, the static
 *   identity JSON and the database overrides columns
 * - Merge layers into the effective Tier with explicit, conservative
 *   semantics per field
 * - Nothing else: resolving WHO a key belongs to belongs to the
 *   stores; enforcing the merged result belongs to the governance
 *
 * Merge semantics (the operator's mental model, made exact):
 * - Scalars (rpm, tpm, max_tokens, monthly_quota, concurrency): the
 *   nearest layer that sets a value wins — key over user over tier.
 *   An override is set or unset; there is no way to say "unlimited"
 *   through a layer, only to not set it.
 * - DeniedModels: the UNION of every layer. Deny is the safety
 *   direction and can only grow; disabling a model for the user
 *   disables it for every one of their keys, while a key may deny
 *   more.
 * - AllowedModels: nil inherits; a list (including the empty list,
 *   which forbids everything) TIGHTENS — layers intersect. No layer
 *   can widen what a parent layer narrowed.
 * - ModelQuotas: shallow map merge, nearest value wins per model.
 */
package auth

import "encoding/json"

// LimitOverride is one layer of limit deviations. Nil/unset fields
// inherit the layer below (the tier, or the user for a key). The same
// JSON shape is the admin API payload and the database overrides
// column, so what an operator sets is byte-identically what is stored.
type LimitOverride struct {
	RPM          *int64 `json:"rpm,omitempty"`
	TPM          *int64 `json:"tpm,omitempty"`
	MaxTokens    *int64 `json:"max_tokens,omitempty"`
	MonthlyQuota *int64 `json:"monthly_quota,omitempty"`
	Concurrency  *int64 `json:"concurrency,omitempty"`
	// AllowedModels: nil inherits; a list tightens by intersection.
	AllowedModels *[]string `json:"allowed_models,omitempty"`
	// DeniedModels unions with the layers below.
	DeniedModels []string `json:"denied_models,omitempty"`
	// ModelQuotas merges per model, nearest value winning.
	ModelQuotas map[string]int64 `json:"model_quotas,omitempty"`
}

// Empty reports whether the override sets nothing at all.
func (o LimitOverride) Empty() bool {
	return o.RPM == nil && o.TPM == nil && o.MaxTokens == nil && o.MonthlyQuota == nil &&
		o.Concurrency == nil && o.AllowedModels == nil && len(o.DeniedModels) == 0 &&
		len(o.ModelQuotas) == 0
}

// ParseOverride decodes one overrides JSON document (the database
// column or the static identity field). Empty input means "no
// override", not an error.
func ParseOverride(raw []byte) (LimitOverride, error) {
	var o LimitOverride
	if len(raw) == 0 {
		return o, nil
	}
	if err := json.Unmarshal(raw, &o); err != nil {
		return LimitOverride{}, err
	}
	return o, nil
}

// MergeTier folds the overrides into the tier in order: earlier
// layers lose to later ones exactly where the semantics say so
// (scalars, per-model quotas); denies only ever grow and allows only
// ever shrink. The result is the snapshot the stores hand out.
func MergeTier(base Tier, overrides ...LimitOverride) Tier {
	merged := base
	merged.ModelQuotas = copyQuotas(base.ModelQuotas)
	merged.DeniedModels = copyStrings(base.DeniedModels)
	merged.AllowedModels = copyStrings(base.AllowedModels)

	for _, o := range overrides {
		if o.RPM != nil {
			merged.RPM = *o.RPM
		}
		if o.TPM != nil {
			merged.TPM = *o.TPM
		}
		if o.MaxTokens != nil {
			merged.MaxTokens = *o.MaxTokens
		}
		if o.MonthlyQuota != nil {
			merged.MonthlyQuota = *o.MonthlyQuota
		}
		if o.Concurrency != nil {
			merged.Concurrency = *o.Concurrency
		}
		merged.DeniedModels = append(merged.DeniedModels, o.DeniedModels...)
		if o.AllowedModels != nil {
			merged.AllowedModels = intersect(merged.AllowedModels, *o.AllowedModels)
		}
		for model, quota := range o.ModelQuotas {
			if merged.ModelQuotas == nil {
				merged.ModelQuotas = make(map[string]int64)
			}
			merged.ModelQuotas[model] = quota
		}
	}
	return merged
}

// intersect tightens the parent allow list by the child list. A child
// list containing the wildcard admits whatever the parent admitted
// (it cannot widen); a child list without the wildcard intersects
// member by member.
func intersect(parent, child []string) []string {
	if len(child) == 0 {
		return []string{}
	}
	hasWildcard := false
	childSet := make(map[string]struct{}, len(child))
	for _, m := range child {
		if m == wildcardModel {
			hasWildcard = true
		}
		childSet[m] = struct{}{}
	}
	if hasWildcard {
		return copyStrings(parent)
	}
	parentSet := make(map[string]struct{}, len(parent))
	for _, m := range parent {
		parentSet[m] = struct{}{}
	}
	// Deterministic order: follow the parent list, then the child list
	// order for entries the parent admitted via its wildcard. The child
	// cannot carry the wildcard here — that case returned above.
	var out []string
	for _, m := range parent {
		if _, ok := childSet[m]; ok {
			out = append(out, m)
		}
	}
	if containsWildcard(parent) {
		for _, m := range child {
			if _, done := parentSet[m]; !done {
				out = append(out, m)
				parentSet[m] = struct{}{}
			}
		}
	}
	if out == nil {
		return []string{}
	}
	return out
}

func containsWildcard(models []string) bool {
	for _, m := range models {
		if m == wildcardModel {
			return true
		}
	}
	return false
}

func copyStrings(in []string) []string {
	if in == nil {
		return nil
	}
	out := make([]string, len(in))
	copy(out, in)
	return out
}

func copyQuotas(in map[string]int64) map[string]int64 {
	if in == nil {
		return nil
	}
	out := make(map[string]int64, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
