/**
 * @file load_validation_test
 * @description The Load rejection contract: every invalid environment
 * value fails loudly at startup with the offending variable named.
 */
package config

import (
	"strings"
	"testing"
)

// TestLoadRejectsInvalidValues: each case sets exactly one variable to
// an invalid value; Load must refuse to start and name the variable.
func TestLoadRejectsInvalidValues(t *testing.T) {
	cases := []struct {
		name    string
		envKey  string
		envVal  string
		wantMsg string // substring the error must carry
		noKey   bool   // true when the error names the entry, not the variable
	}{
		{"bad shutdown grace", envShutdownGrace, "soon", "parse", false},
		{"zero shutdown grace", envShutdownGrace, "0s", "must be positive", false},
		{"negative shutdown grace", envShutdownGrace, "-1s", "must be positive", false},
		{"bad queue size", envAccessLogQueue, "many", "parse", false},
		{"zero queue size", envAccessLogQueue, "0", "must be positive", false},
		{"negative queue size", envAccessLogQueue, "-4", "must be positive", false},
		{"bad upstream JSON", envUpstreams, "{", "parse", true},
		{"upstream without id", envUpstreams, `[{"base_url":"http://x","models":["m"]}]`, "needs id and base_url", true},
		{"upstream without base url", envUpstreams, `[{"id":"u","models":["m"]}]`, "needs id and base_url", true},
		{"upstream without models", envUpstreams, `[{"id":"u","base_url":"http://x","models":[]}]`, "lists no models", true},
		{"bad retry attempts", envRetryMaxAttempts, "once", "parse", false},
		{"zero retry attempts", envRetryMaxAttempts, "0", "must be positive", false},
		{"bad attempt timeout", envRetryAttemptTimeout, "10", "parse", false},
		{"bad overall deadline", envRetryOverall, "forever", "parse", false},
		{"bad backoff initial", envRetryBackoffInitial, "quickly", "parse", false},
		{"bad backoff max", envRetryBackoffMax, "huge", "parse", false},
		{"bad retry budget", envRetryBudget, "plenty", "parse", false},
		{"negative retry budget", envRetryBudget, "-1", "must not be negative", false},
		{"bad stream timeout", envStreamTimeout, "slow", "parse", false},
		{"negative stream timeout", envStreamTimeout, "-1s", "must not be negative", false},
		{"bad reconcile interval", envReconcileInterval, "later", "parse", false},
		{"negative reconcile interval", envReconcileInterval, "-1m", "must not be negative", false},
		{"bad cache enabled", envCacheEnabled, "maybe", "parse", false},
		{"bad cache ttl", envCacheTTL, "infinite", "parse", false},
		{"zero cache ttl", envCacheTTL, "0s", "must be positive", false},
		{"negative cache ttl", envCacheTTL, "-1s", "must be positive", false},
		{"bad cache capacity", envCacheCapacity, "lots", "parse", false},
		{"zero cache capacity", envCacheCapacity, "0", "must be positive", false},
		{"bad circuit enabled", envCircuitEnabled, "perhaps", "parse", false},
		{"bad circuit threshold", envCircuitThreshold, "many", "parse", false},
		{"zero circuit threshold", envCircuitThreshold, "0", "must be positive", false},
		{"bad circuit cooldown", envCircuitCooldown, "ages", "parse", false},
		{"zero circuit cooldown", envCircuitCooldown, "0s", "must be positive", false},
		{"bad circuit probe", envCircuitProbe, "soon", "parse", false},
		{"zero circuit probe", envCircuitProbe, "0s", "must be positive", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cleanEnv(t)
			t.Setenv(tc.envKey, tc.envVal)

			cfg, err := Load()
			if err == nil {
				t.Fatalf("Load accepted %s=%q, config %+v", tc.envKey, tc.envVal, cfg)
			}
			if !strings.Contains(err.Error(), tc.wantMsg) {
				t.Fatalf("error = %q, want it to contain %q", err, tc.wantMsg)
			}
			if !tc.noKey && !strings.Contains(err.Error(), tc.envKey) {
				t.Fatalf("error = %q, want it to name %s", err, tc.envKey)
			}
		})
	}
}

// TestLoadAcceptsZeroBudgetAndIntervals: zero is a valid "disabled"
// value for the retry budget, stream timeout and reconcile interval —
// only negatives are rejected.
func TestLoadAcceptsZeroBudgetAndIntervals(t *testing.T) {
	cleanEnv(t)
	t.Setenv(envRetryBudget, "0")
	t.Setenv(envStreamTimeout, "0s")
	t.Setenv(envReconcileInterval, "0s")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load rejected zero-disabled values: %v", err)
	}
	if cfg.Retry.BudgetMaxInFlight != 0 || cfg.Retry.StreamTimeout != 0 || cfg.ReconcileInterval != 0 {
		t.Fatalf("zero values lost: %+v interval %s", cfg.Retry, cfg.ReconcileInterval)
	}
}
