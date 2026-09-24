/**
 * @file auth_test
 * @description Tier contract tests: the fail-closed model authorization
 * the schema and identity snapshot promise.
 */
package auth

import "testing"

func TestTierAllowsModel(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		models  []string
		request string
		want    bool
	}{
		{"empty list allows nothing (fail-closed)", nil, "m1", false},
		{"exact match", []string{"m1", "m2"}, "m2", true},
		{"unlisted model denied", []string{"m1"}, "m9", false},
		{"wildcard admits everything", []string{"*"}, "anything", true},
		{"wildcard plus list", []string{"m1", "*"}, "anything", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tier := Tier{AllowedModels: tc.models}
			if got := tier.AllowsModel(tc.request); got != tc.want {
				t.Fatalf("AllowsModel(%q) with %v = %v, want %v", tc.request, tc.models, got, tc.want)
			}
		})
	}
}
