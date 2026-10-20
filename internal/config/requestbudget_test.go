/**
 * @file requestbudget_test
 * @description The derived request budget and lease horizon against
 * individually legal but collectively overflowing duration values.
 */
package config

import (
	"strings"
	"testing"
)

// TestRequestBudgetSaturatesWhenBoundsSumPastTheCeiling pins the
// overflow behavior of the derived request budget: two duration values
// Load accepts individually (each under the parse ceiling) can overflow
// when summed. The request ceiling and the derived lease horizon must
// saturate instead of wrapping negative — a negative budget would
// expire every context it bounds instantly and hand the ledger an
// immediately-sweepable lease TTL, refunding requests that are still
// spending.
func TestRequestBudgetSaturatesWhenBoundsSumPastTheCeiling(t *testing.T) {
	cleanEnv(t)
	// Each parses within the Duration ceiling (2562047h); their sum
	// does not fit.
	t.Setenv(envRetryOverall, "1600000h")
	t.Setenv(envStreamTimeout, "1600000h")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load with individually valid, jointly overflowing bounds: %v", err)
	}
	if cfg.RequestCeiling() <= 0 {
		t.Fatalf("request ceiling = %v, want a saturated positive bound", cfg.RequestCeiling())
	}
	if cfg.LeaseTTL() <= 0 {
		t.Fatalf("lease TTL = %v, want a saturated positive horizon", cfg.LeaseTTL())
	}
}

// TestLeaseTTLLessThanSaturatedBudgetIsRefused: a configured horizon
// that cannot cover the saturated request budget must still fail the
// load loudly — an unbounded-looking budget is exactly the deployment
// whose live reservations the short horizon would silently refund.
func TestLeaseTTLLessThanSaturatedBudgetIsRefused(t *testing.T) {
	cleanEnv(t)
	t.Setenv(envRetryOverall, "1600000h")
	t.Setenv(envStreamTimeout, "1600000h")
	t.Setenv(envQuotaLeaseTTL, "10m")

	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "must exceed the longest request") {
		t.Fatalf("err = %v, want the lease-horizon refusal", err)
	}
}
