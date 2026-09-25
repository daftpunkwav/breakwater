/**
 * @file memory
 * @description The in-process quota ledger: the reference
 * implementation of the lease semantics.
 *
 * Responsibilities:
 * - The reference behavior every other backend must match: atomic
 *   reserve, refund-only settle, idempotent terminal transitions
 * - Serve tests and the in-memory degradation posture
 *
 * Terminal leases are retained for audit (settle-after-terminal stays
 * observable) but only for the same bounded audit window the Redis
 * backend applies: the sweeper purges records past it, so the lease map
 * cannot grow with request volume.
 */
package quota

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Memory is the in-process Ledger. It is safe for concurrent use.
type Memory struct {
	mu       sync.Mutex
	balances map[string]int64
	leases   map[string]*memLease
	now      func() time.Time
	// leaseTTL matches the Redis backend's sweep TTL.
	leaseTTL time.Duration
}

// memLease is the stored lease plus the in-memory backend's audit
// bookkeeping: when the lease reached a terminal state. Zero while the
// lease is RESERVED.
type memLease struct {
	Lease
	terminalAt time.Time
}

// NewMemory builds the in-process ledger.
func NewMemory() *Memory {
	return &Memory{
		balances: make(map[string]int64),
		leases:   make(map[string]*memLease),
		now:      time.Now,
		leaseTTL: defaultLeaseTTL,
	}
}

// WithClock overrides the clock for tests.
func (m *Memory) WithClock(now func() time.Time) *Memory {
	m.now = now
	return m
}

// SetBalance implements Ledger: provisions or resets a tenant balance
// (admin surface).
func (m *Memory) SetBalance(_ context.Context, tenantID string, balance int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.balances[tenantID] = balance
	return nil
}

// EnsureBalance provisions the balance only when the tenant has none;
// it reports whether it created the balance.
func (m *Memory) EnsureBalance(_ context.Context, tenantID string, initial int64) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.balances[tenantID]; ok {
		return false, nil
	}
	m.balances[tenantID] = initial
	return true, nil
}

// Reserve implements Ledger.
func (m *Memory) Reserve(_ context.Context, tenantID string, amount int64) (Lease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	balance, ok := m.balances[tenantID]
	if !ok || balance < amount {
		return Lease{}, ErrInsufficientBalance
	}
	m.balances[tenantID] = balance - amount
	lease := Lease{
		ID:        newLeaseID(),
		TenantID:  tenantID,
		Amount:    amount,
		State:     LeaseStateReserved,
		CreatedAt: m.now(),
	}
	m.leases[lease.ID] = &memLease{Lease: lease}
	return lease, nil
}

// Settle implements Ledger.
func (m *Memory) Settle(_ context.Context, leaseID string, usedTokens int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	lease, ok := m.leases[leaseID]
	if !ok {
		return fmt.Errorf("quota: unknown lease %s", leaseID)
	}
	if lease.State != LeaseStateReserved {
		// Detectable no-op: late settles after sweeper expiry are
		// expected; nothing moves. The contract requires surfacing them.
		slog.Info("quota settle reached a terminal lease", "lease", leaseID, "state", lease.State)
		return nil
	}
	refund := lease.Amount - usedTokens
	if refund > 0 {
		m.balances[lease.TenantID] += refund
	}
	lease.State = LeaseStateSettled
	lease.terminalAt = m.now()
	return nil
}

// Cancel implements Ledger.
func (m *Memory) Cancel(_ context.Context, leaseID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	lease, ok := m.leases[leaseID]
	if !ok {
		return fmt.Errorf("quota: unknown lease %s", leaseID)
	}
	if lease.State != LeaseStateReserved {
		// Detectable no-op, surfaced per the Ledger contract.
		slog.Info("quota cancel reached a terminal lease", "lease", leaseID, "state", lease.State)
		return nil
	}
	m.balances[lease.TenantID] += lease.Amount
	lease.State = LeaseStateCancelled
	lease.terminalAt = m.now()
	return nil
}

// Balance implements Ledger.
func (m *Memory) Balance(_ context.Context, tenantID string) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	balance, ok := m.balances[tenantID]
	if !ok {
		// Unprovisioned tenants are reported, not read as zero: the
		// admin API must tell "no ledger" apart from "drained".
		return 0, ErrUnknownTenant
	}
	return balance, nil
}

// SweepOnce reclaims RESERVED leases older than the lease TTL, refunding
// their amounts and marking them EXPIRED; it returns how many leases
// were reclaimed (bounded by limit per call). Terminal leases past the
// shared audit window (leaseAuditTTL, the Redis backend's retention) are
// purged in the same pass — reclaim count excludes purges, and purges
// never touch balances.
func (m *Memory) SweepOnce(_ context.Context, now time.Time, limit int) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	reservedCutoff := now.Add(-m.leaseTTL)
	terminalCutoff := now.Add(-leaseAuditTTL)
	expired := 0
	for id, lease := range m.leases {
		if lease.State != LeaseStateReserved {
			if lease.terminalAt.Before(terminalCutoff) {
				delete(m.leases, id)
			}
			continue
		}
		if expired >= limit || !lease.CreatedAt.Before(reservedCutoff) {
			continue
		}
		m.balances[lease.TenantID] += lease.Amount
		lease.State = LeaseStateExpired
		lease.terminalAt = now
		expired++
	}
	return expired, nil
}

// newLeaseID generates a random lease identifier.
func newLeaseID() string {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		panic(fmt.Sprintf("quota: lease id entropy unavailable: %v", err))
	}
	return hex.EncodeToString(buf[:])
}
