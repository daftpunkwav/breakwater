/**
 * @file load_validation_test
 * @description The Load rejection contract: every invalid environment
 * value fails loudly at startup with the offending variable named.
 */
package config

import (
	"strings"
	"testing"
	"time"
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
		{"negative attempt timeout", envRetryAttemptTimeout, "-5s", "must not be negative", false},
		{"negative overall deadline", envRetryOverall, "-1m", "must not be negative", false},
		{"negative backoff initial", envRetryBackoffInitial, "-100ms", "must not be negative", false},
		{"negative backoff max", envRetryBackoffMax, "-2s", "must not be negative", false},
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
		{"unknown routing strategy", envRouting, "cheapest", "static", false},
		{"bad probe interval", envProbeInterval, "soon", "parse", false},
		{"negative probe interval", envProbeInterval, "-1s", "must not be negative", false},
		{"bad probe timeout", envProbeTimeout, "quickly", "parse", false},
		{"zero probe timeout while enabled", envProbeTimeout, "0s", "must be positive", false},
		{"bad probe threshold", envProbePasses, "lots", "parse", false},
		{"zero probe threshold while enabled", envProbePasses, "0", "must be positive", false},
		{"bad fallbacks JSON", envFallbacks, "{", "parse", true},
		{"empty fallback key", envFallbacks, `{"":["m2"]}`, "empty model name", true},
		{"empty fallback chain", envFallbacks, `{"m1":[]}`, "lists no fallbacks", true},
		{"empty fallback entry", envFallbacks, `{"m1":[""]}`, "empty fallback", true},
		{"bad context limits JSON", envContextLimits, "{", "parse", true},
		{"empty context limit key", envContextLimits, `{"":100}`, "empty model name", true},
		{"zero context limit", envContextLimits, `{"m1":0}`, "must be positive", true},
		{"negative context limit", envContextLimits, `{"m1":-5}`, "must be positive", true},
		{"empty model alias half", envUpstreams, `[{"id":"u","base_url":"http://x","models":["=real"]}]`, "invalid model binding", true},
		{"empty real alias half", envUpstreams, `[{"id":"u","base_url":"http://x","models":["client="]}]`, "invalid model binding", true},
		{"wildcard alias", envUpstreams, `[{"id":"u","base_url":"http://x","models":["*=real"]}]`, "invalid model binding", true},
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
// value for the retry budget, stream timeout, reconcile interval and
// the recovery probe — only negatives are rejected.
func TestLoadAcceptsZeroBudgetAndIntervals(t *testing.T) {
	cleanEnv(t)
	t.Setenv(envRetryBudget, "0")
	t.Setenv(envStreamTimeout, "0s")
	t.Setenv(envReconcileInterval, "0s")
	t.Setenv(envProbeInterval, "0s")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load rejected zero-disabled values: %v", err)
	}
	if cfg.Retry.BudgetMaxInFlight != 0 || cfg.Retry.StreamTimeout != 0 || cfg.ReconcileInterval != 0 {
		t.Fatalf("zero values lost: %+v interval %s", cfg.Retry, cfg.ReconcileInterval)
	}
	if cfg.Probe.Interval != 0 {
		t.Fatalf("probe interval = %s, want 0 (disabled)", cfg.Probe.Interval)
	}
}

// TestLoadLeaseTTLMustOutlastTheRequest: a reclaim horizon shorter than
// the longest request the gateway will run silently refunds a request
// that really spent tokens, so Load refuses it by name.
func TestLoadLeaseTTLMustOutlastTheRequest(t *testing.T) {
	t.Setenv(envRetryOverall, "1m")
	t.Setenv(envStreamTimeout, "10m")
	t.Setenv(envQuotaLeaseTTL, "5m")
	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), envQuotaLeaseTTL) {
		t.Fatalf("err = %v, want a %s rejection", err, envQuotaLeaseTTL)
	}
}

// TestLeaseTTLDerivesFromTheRequestBudget: with no explicit value the
// horizon covers the attempt phase plus a committed stream's body, with
// headroom for the sweeper's own interval.
func TestLeaseTTLDerivesFromTheRequestBudget(t *testing.T) {
	cfg := Config{Retry: Retry{OverallDeadline: time.Minute, StreamTimeout: 10 * time.Minute}}
	want := 11*time.Minute + leaseTTLHeadroom
	if got := cfg.LeaseTTL(); got != want {
		t.Fatalf("LeaseTTL = %v, want %v", got, want)
	}
}

// TestLeaseTTLUnboundedRequestUsesTheGenerousHorizon: a client that
// owns an unbounded stream leaves nothing to derive from, so the
// fallback must not be the old ten minutes — that is the horizon which
// would refund such a request mid-flight.
func TestLeaseTTLUnboundedRequestUsesTheGenerousHorizon(t *testing.T) {
	cfg := Config{Retry: Retry{StreamTimeout: 0, OverallDeadline: time.Minute}}
	if got := cfg.LeaseTTL(); got != unboundedRequestLeaseTTL {
		t.Fatalf("LeaseTTL = %v, want %v", got, unboundedRequestLeaseTTL)
	}
}

// TestLoadAcceptsAnExplicitHorizonAboveTheBudget: an operator who wants
// a different horizon may set one, as long as it is large enough.
func TestLoadAcceptsAnExplicitHorizonAboveTheBudget(t *testing.T) {
	t.Setenv(envRetryOverall, "1m")
	t.Setenv(envStreamTimeout, "10m")
	t.Setenv(envQuotaLeaseTTL, "30m")
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.LeaseTTL() != 30*time.Minute {
		t.Fatalf("LeaseTTL = %v, want the configured 30m", cfg.LeaseTTL())
	}
}

// TestLoadRejectsNegativeLeaseTTL: a negative reclaim horizon is not a
// horizon, and must be named at startup rather than silently falling back
// to the derivation.
func TestLoadRejectsNegativeLeaseTTL(t *testing.T) {
	t.Setenv(envQuotaLeaseTTL, "-1s")
	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "must not be negative") {
		t.Fatalf("err = %v, want a negative-lease-TTL rejection", err)
	}
}

// TestRequestCeiling: the bound work that must outlive its client runs
// under. A request with no declared upper limit still gets a usable
// ceiling rather than "unlimited", so a caller never has to special-case
// zero.
func TestRequestCeiling(t *testing.T) {
	bounded := Config{Retry: Retry{OverallDeadline: time.Minute, StreamTimeout: 10 * time.Minute}}
	if got := bounded.RequestCeiling(); got != 11*time.Minute {
		t.Fatalf("RequestCeiling = %v, want 11m", got)
	}
	for _, cfg := range []Config{
		{Retry: Retry{StreamTimeout: 0, OverallDeadline: time.Minute}},
		{Retry: Retry{StreamTimeout: 10 * time.Minute, OverallDeadline: 0}},
		{},
	} {
		if got := cfg.RequestCeiling(); got != unboundedRequestLeaseTTL {
			t.Fatalf("unbounded RequestCeiling = %v, want %v", got, unboundedRequestLeaseTTL)
		}
	}
}

func TestLoadRejectsBrokenRetryBudgetShare(t *testing.T) {
	cases := []struct {
		name string
		env  string
		want string
	}{
		{"percent above 100", "BREAKWATER_RETRY_BUDGET_PERCENT=150", "percentage in [0, 100]"},
		{"negative percent", "BREAKWATER_RETRY_BUDGET_PERCENT=-5", "percentage in [0, 100]"},
		{"floor without percent", "BREAKWATER_RETRY_BUDGET_PERCENT=20\nBREAKWATER_RETRY_BUDGET_MIN_IN_FLIGHT=0", "must be at least 1"},
	}
	for _, tc := range cases {
		for _, line := range strings.Split(tc.env, "\n") {
			key, value, _ := strings.Cut(line, "=")
			t.Setenv(key, value)
		}
		if _, err := Load(); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v, want %q", tc.name, err, tc.want)
		}
	}
}
