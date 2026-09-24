/**
 * @file eligibility_test
 * @description Cache eligibility rules: only explicit determinism is
 * cacheable (PRD Q3).
 */
package cache

import (
	"testing"

	"github.com/daftpunkwav/breakwater/internal/protocol"
)

func floatPtr(f float64) *float64 { return &f }
func intPtr(i int) *int           { return &i }

func TestEligible(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name        string
		temperature *float64
		topP        *float64
		n           *int
		eligible    bool
	}{
		{"explicit zero temperature", floatPtr(0), nil, nil, true},
		{"default temperature (absent)", nil, nil, nil, false},
		{"nonzero temperature", floatPtr(0.7), nil, nil, false},
		{"explicit zero with top_p 1", floatPtr(0), floatPtr(1), nil, true},
		{"explicit zero with narrower top_p", floatPtr(0), floatPtr(0.9), nil, false},
		{"multiple choices", floatPtr(0), nil, intPtr(3), false},
		{"single choice", floatPtr(0), nil, intPtr(1), true},
	}
	for _, tc := range cases {
		req := protocol.ChatRequest{Temperature: tc.temperature, TopP: tc.topP, N: tc.n}
		if got := Eligible(req); got != tc.eligible {
			t.Errorf("%s: eligible = %v, want %v", tc.name, got, tc.eligible)
		}
	}
}
