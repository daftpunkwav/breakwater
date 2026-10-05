/**
 * @file seedbalances_test
 * @description Balance provisioning from the static identity set: which
 * tenants get a ledger, what a zero-budget tier looks like, and the two
 * ledgers the seeder must tolerate.
 */
package main

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/daftpunkwav/breakwater/internal/auth"
	"github.com/daftpunkwav/breakwater/internal/quota"
)

// newSingleTenantIdentity builds the one-tenant static identity the
// seeder and governance-assembly tests share: tier "free" with a
// 100-token monthly quota, tenant "t" holding key "k".
func newSingleTenantIdentity(t *testing.T) *auth.Static {
	t.Helper()
	identity, err := auth.NewStatic(auth.StaticConfig{
		Tiers:   []auth.StaticTier{{ID: "free", MonthlyQuota: 100}},
		Tenants: []auth.StaticTenant{{ID: "t", Name: "T", Tier: "free", Keys: []string{"k"}}},
	})
	if err != nil {
		t.Fatalf("identity: %v", err)
	}
	return identity
}

// seedProvisionedLedger provisions a fresh memory ledger from the given
// static identity configuration, the shape the provisioning tests
// inspect.
func seedProvisionedLedger(t *testing.T, cfg auth.StaticConfig) *quota.Memory {
	t.Helper()
	identity, err := auth.NewStatic(cfg)
	if err != nil {
		t.Fatalf("identity: %v", err)
	}
	ledger := quota.NewMemory()
	seedBalances(context.Background(), identity, ledger, slog.New(slog.DiscardHandler))
	return ledger
}

// TestSeedBalancesProvisionsZeroQuotaTiers: a tier may legitimately
// budget nothing. Provisioning a zero balance is what keeps that a
// spending decision (402, the budget is gone) rather than a provisioning
// fault (503, the tenant has no ledger) — the seeder must not skip it.
func TestSeedBalancesProvisionsZeroQuotaTiers(t *testing.T) {
	t.Parallel()
	ledger := seedProvisionedLedger(t, auth.StaticConfig{
		Tiers:   []auth.StaticTier{{ID: "free", MonthlyQuota: 0}},
		Tenants: []auth.StaticTenant{{ID: "t0", Name: "T0", Tier: "free", Keys: []string{"k"}}},
	})

	ctx := context.Background()
	if bal, err := ledger.Balance(ctx, "t0"); err != nil || bal != 0 {
		t.Fatalf("balance = %d err = %v, want a provisioned zero", bal, err)
	}
	if _, err := ledger.Reserve(ctx, "t0", 1); !errors.Is(err, quota.ErrInsufficientBalance) {
		t.Fatalf("reserve = %v, want ErrInsufficientBalance: a zero budget is not a missing account", err)
	}
	// A tenant the identity set never declared is still unprovisioned.
	if _, err := ledger.Reserve(ctx, "ghost", 1); !errors.Is(err, quota.ErrUnknownTenant) {
		t.Fatalf("undeclared tenant reserve = %v, want ErrUnknownTenant", err)
	}
}

// seedLedgerFailed wraps a ledger whose EnsureBalance always fails.
type seedLedgerFailed struct {
	quota.Ledger
	err error
}

func (l seedLedgerFailed) EnsureBalance(context.Context, string, int64) (bool, error) {
	return false, l.err
}

// seedLedgerOpaque hides EnsureBalance: to seedBalances it is a ledger
// without provisioning support, which must be skipped silently.
type seedLedgerOpaque struct {
	quota.Ledger
}

func TestSeedBalancesEdgeLedgers(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.DiscardHandler)
	identity := newSingleTenantIdentity(t)

	// A failing seeder is logged, never panics.
	seedBalances(context.Background(), identity,
		seedLedgerFailed{Ledger: quota.NewMemory(), err: errors.New("redis down")}, logger)

	// A ledger without provisioning support is skipped.
	seedBalances(context.Background(), identity,
		seedLedgerOpaque{Ledger: quota.NewMemory()}, logger)
}
