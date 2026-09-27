/**
 * @file routing_config_test
 * @description The assembly-time validation of the model-level routing
 * configuration: fallback chains and context ceilings must name
 * client-facing models the configuration actually serves.
 */
package main

import (
	"strings"
	"testing"
)

func TestValidateModelNames(t *testing.T) {
	t.Parallel()
	known := []string{"m1", "m2"}

	if err := validateModelNames(known, map[string][]string{"m1": {"m2"}}, map[string]int64{"m2": 100}); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	if err := validateModelNames(known, nil, nil); err != nil {
		t.Fatalf("empty config rejected: %v", err)
	}

	if err := validateModelNames(known, map[string][]string{"typo": {"m2"}}, nil); err == nil ||
		!strings.Contains(err.Error(), "fallback key") {
		t.Fatalf("err = %v, want a fallback-key rejection", err)
	}
	if err := validateModelNames(known, map[string][]string{"m1": {"ghost"}}, nil); err == nil ||
		!strings.Contains(err.Error(), "fallback target") {
		t.Fatalf("err = %v, want a fallback-target rejection", err)
	}
	if err := validateModelNames(known, nil, map[string]int64{"ghost": 10}); err == nil ||
		!strings.Contains(err.Error(), "context limit") {
		t.Fatalf("err = %v, want a context-limit rejection", err)
	}
}
