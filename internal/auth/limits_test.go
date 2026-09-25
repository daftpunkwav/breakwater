/**
 * @file limits_test
 * @description The layered merge semantics: scalar precedence (key
 * over user over tier), the deny union, the allow tightening and the
 * per-model quota merge.
 */
package auth

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestMergeTierScalarsNearestLayerWins(t *testing.T) {
	t.Parallel()
	keyRPM := int64(10)
	userQuota := int64(5000)
	base := Tier{ID: "t", RPM: 100, TPM: 200, MonthlyQuota: 1000}
	got := MergeTier(base,
		LimitOverride{MonthlyQuota: &userQuota}, // user layer
		LimitOverride{RPM: &keyRPM},             // key layer
	)
	if got.RPM != 10 || got.TPM != 200 {
		t.Fatalf("rpm=%d tpm=%d, want key rpm and tier tpm", got.RPM, got.TPM)
	}
	if got.MonthlyQuota != 5000 {
		t.Fatalf("quota = %d, want the user layer's 5000", got.MonthlyQuota)
	}
}

func TestMergeTierDenyUnions(t *testing.T) {
	t.Parallel()
	base := Tier{AllowedModels: []string{"*"}}
	got := MergeTier(base, LimitOverride{DeniedModels: []string{"m1"}}, LimitOverride{DeniedModels: []string{"m1", "m2"}})
	if got.AllowsModel("m1") || got.AllowsModel("m2") {
		t.Fatalf("denied models still allowed: %v", got.DeniedModels)
	}
	if !got.AllowsModel("m3") {
		t.Fatal("an unrelated model must stay allowed")
	}
	if !reflect.DeepEqual(got.DeniedModels, []string{"m1", "m1", "m2"}) {
		t.Fatalf("denied = %v, want the union in order", got.DeniedModels)
	}
}

// TestMergeTierDenyBeatsAllow: the deny check runs before the allow
// list, whatever every layer says.
func TestMergeTierDenyBeatsAllow(t *testing.T) {
	t.Parallel()
	base := Tier{AllowedModels: []string{"a", "b"}}
	got := MergeTier(base, LimitOverride{DeniedModels: []string{"a"}})
	if got.AllowsModel("a") {
		t.Fatal("deny must win over the allow list")
	}
	if !got.AllowsModel("b") {
		t.Fatal("b must survive")
	}
	// The wildcard deny is the operator's master switch.
	all := MergeTier(Tier{AllowedModels: []string{"*"}}, LimitOverride{DeniedModels: []string{"*"}})
	if all.AllowsModel("anything") {
		t.Fatal("a wildcard deny must forbid every model")
	}
}

// TestMergeTierAllowTightensOnly: a child allow list narrows the
// parent; the wildcard inherits the parent's list without widening.
func TestMergeTierAllowTightensOnly(t *testing.T) {
	t.Parallel()
	base := Tier{AllowedModels: []string{"a", "b", "c"}}

	narrow := MergeTier(base, LimitOverride{AllowedModels: &[]string{"a", "b"}})
	if narrow.AllowsModel("c") || !narrow.AllowsModel("a") {
		t.Fatalf("narrowing failed: %v", narrow.AllowedModels)
	}

	// A key cannot widen what the user narrowed.
	widenAttempt := MergeTier(
		MergeTier(base, LimitOverride{AllowedModels: &[]string{"a"}}),
		LimitOverride{AllowedModels: &[]string{"a", "b"}})
	if widenAttempt.AllowsModel("b") {
		t.Fatalf("child widened the parent: %v", widenAttempt.AllowedModels)
	}

	// The wildcard child inherits the parent list verbatim.
	inherited := MergeTier(base, LimitOverride{AllowedModels: &[]string{"*"}})
	if !reflect.DeepEqual(inherited.AllowedModels, []string{"a", "b", "c"}) {
		t.Fatalf("wildcard child = %v, want the parent list", inherited.AllowedModels)
	}

	// The explicit empty list forbids everything.
	none := MergeTier(base, LimitOverride{AllowedModels: &[]string{}})
	if none.AllowsModel("a") || len(none.AllowedModels) != 0 {
		t.Fatalf("empty allow = %v, want an explicit forbid-all", none.AllowedModels)
	}
}

// TestMergeTierWildcardParentChild: a wildcard parent plus a named
// child admits exactly the named child set.
func TestMergeTierWildcardParentChild(t *testing.T) {
	t.Parallel()
	base := Tier{AllowedModels: []string{"*"}}
	got := MergeTier(base, LimitOverride{AllowedModels: &[]string{"m1", "m2"}})
	if !got.AllowsModel("m1") || !got.AllowsModel("m2") || got.AllowsModel("m3") {
		t.Fatalf("wildcard parent + named child = %v, want exactly the child set", got.AllowedModels)
	}
}

func TestMergeTierModelQuotasShallowMerge(t *testing.T) {
	t.Parallel()
	base := Tier{ModelQuotas: map[string]int64{"m1": 100, "m2": 200}}
	got := MergeTier(base,
		LimitOverride{ModelQuotas: map[string]int64{"m1": 150}},
		LimitOverride{ModelQuotas: map[string]int64{"m3": 50}},
	)
	want := map[string]int64{"m1": 150, "m2": 200, "m3": 50}
	if !reflect.DeepEqual(got.ModelQuotas, want) {
		t.Fatalf("quotas = %v, want %v", got.ModelQuotas, want)
	}
	// The base map must not have been mutated by the merge.
	if base.ModelQuotas["m1"] != 100 || len(base.ModelQuotas) != 2 {
		t.Fatalf("base mutated: %v", base.ModelQuotas)
	}
}

func TestParseOverrideEmptyInputIsNoOverride(t *testing.T) {
	t.Parallel()
	o, err := ParseOverride(nil)
	if err != nil || !o.Empty() {
		t.Fatalf("o = %+v err = %v, want an empty override", o, err)
	}
}

// TestParseOverrideJSONRoundTrip: the wire shape the admin API speaks
// is the shape the database stores.
func TestParseOverrideJSONRoundTrip(t *testing.T) {
	t.Parallel()
	raw := []byte(`{"rpm":10,"concurrency":2,"allowed_models":["a"],"denied_models":["b"],"model_quotas":{"a":9}}`)
	o, err := ParseOverride(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if o.RPM == nil || *o.RPM != 10 || o.Concurrency == nil || *o.Concurrency != 2 {
		t.Fatalf("scalars lost: %+v", o)
	}
	if o.AllowedModels == nil || len(*o.AllowedModels) != 1 {
		t.Fatalf("allow list lost: %+v", o.AllowedModels)
	}
	// Omitted optional fields stay nil — the inherit signal.
	if o.TPM != nil || o.MonthlyQuota != nil {
		t.Fatalf("unset fields must stay nil: %+v", o)
	}
	out, err := json.Marshal(o)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	again, err := ParseOverride(out)
	if err != nil || !reflect.DeepEqual(o, again) {
		t.Fatalf("round trip = %+v err = %v", again, err)
	}
}
