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
 * observable) for leaseAuditTTL, the same window the Redis ledger uses.
 * The mechanism differs: Redis expires each record by its own key TTL,
 * while here the sweeper drops terminal records past that age — so this
 * bound holds only while the sweeper is running.
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

// defaultLeaseTTL is the fallback reclaim horizon for a ledger built
// without an explicit one. It only holds when the request budget stays
// well inside it; the assembly computes the real value from the
// configured request timeouts, because a horizon shorter than the
// longest possible request refunds a request that really spent tokens.
const defaultLeaseTTL = 10 * time.Minute

// NewMemory builds the in-process ledger.
func NewMemory() *Memory {
	return &Memory{
		balances: make(map[string]int64),
		leases:   make(map[string]*memLease),
		now:      time.Now,
		leaseTTL: defaultLeaseTTL,
	}
}

// WithLeaseTTL sets how long a RESERVED lease may live before the
// sweeper reclaims it. It must outlast the longest request the gateway
// will run, or a live request's reservation is refunded while it is
// still spending tokens.
func (m *Memory) WithLeaseTTL(d time.Duration) *Memory {
	if d > 0 {
		m.leaseTTL = d
	}
	return m
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
	if !ok {
		return Lease{}, ErrUnknownTenant
	}
	if balance < amount {
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
	// The usage figure originates from the upstream reply; a broken or
	// hostile upstream reporting a negative count must not mint balance:
	// settlement only ever refunds what the lease reserved, never more.
	used := max(usedTokens, 0)
	refund := lease.Amount - used
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

// scanBatch caps how many leases one SweepOnce pass walks. The lock is
// shared with every Reserve/Settle/Cancel, so a pass must stay bounded
// no matter how many leases have accumulated. Go randomizes map
// iteration order per range, so successive passes converge on the whole
// set in practice, but a given entry's coverage is probabilistic rather
// than guaranteed. The Redis ledger bounds the same pass with a zset
// page; this is the memory-side equivalent.
const scanBatch = 8192

// SweepOnce reclaims RESERVED leases older than the lease TTL, refunding
// their amounts and marking them EXPIRED; it returns how many leases
// were reclaimed, never more than limit. That bound caps the reclaim
// count only — a pass that reaches it keeps walking. Terminal leases
// past leaseAuditTTL are dropped in the same walk; purges are never
// counted as reclaims and never touch balances. One pass examines at
// most scanBatch leases.
func (m *Memory) SweepOnce(_ context.Context, now time.Time, limit int) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	reservedCutoff := now.Add(-m.leaseTTL)
	terminalCutoff := now.Add(-leaseAuditTTL)
	expired := 0
	scanned := 0
	for id, lease := range m.leases {
		if scanned >= scanBatch {
			break
		}
		scanned++
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
