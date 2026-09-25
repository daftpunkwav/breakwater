/**
 * @file auth_test
 * @description Tier contract tests: the fail-closed allow rule and the
 * deny-wins-over-allow matrix the schema and identity snapshot promise.
 */
package auth

import "testing"

func TestTierAllowsModel(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		allowed []string
		denied  []string
		request string
		want    bool
	}{
		{"empty list allows nothing (fail-closed)", nil, nil, "m1", false},
		{"exact match", []string{"m1", "m2"}, nil, "m2", true},
		{"unlisted model denied", []string{"m1"}, nil, "m9", false},
		{"wildcard admits everything", []string{"*"}, nil, "anything", true},
		{"wildcard plus list", []string{"m1", "*"}, nil, "anything", true},
		{"named deny removes the model from the allow list", []string{"m1", "m2"}, []string{"m2"}, "m2", false},
		{"named deny spares the other listed model", []string{"m1", "m2"}, []string{"m2"}, "m1", true},
		{"wildcard deny forbids all", []string{"*"}, []string{"*"}, "anything", false},
		{"deny list decides with no allow list", nil, []string{"m1"}, "m1", false},
		{"unrelated model survives a named deny", []string{"m1"}, []string{"m2"}, "m1", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tier := Tier{AllowedModels: tc.allowed, DeniedModels: tc.denied}
			if got := tier.AllowsModel(tc.request); got != tc.want {
				t.Fatalf("AllowsModel(%q) with allow %v deny %v = %v, want %v",
					tc.request, tc.allowed, tc.denied, got, tc.want)
			}
		})
	}
}
