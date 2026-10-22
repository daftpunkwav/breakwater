/**
 * @file requestbudget_test
 * @description The derived request budget and lease horizon against
 * individually legal but collectively overflowing duration values.
 */
package config

import (
	"math"
	"strings"
	"testing"
	"time"
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
	// Both accessors must read the exact saturation ceiling: a merely
	// positive check would accept a wrapped or clamped-off value.
	want := time.Duration(math.MaxInt64)
	if cfg.RequestCeiling() != want {
		t.Fatalf("request ceiling = %v, want %v", cfg.RequestCeiling(), want)
	}
	if cfg.LeaseTTL() != want {
		t.Fatalf("lease TTL = %v, want %v", cfg.LeaseTTL(), want)
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
