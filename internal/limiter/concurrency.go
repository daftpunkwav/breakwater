/**
 * @file concurrency
 * @description The per-tenant concurrency ceiling: a hand-written
 * counting semaphore, one slot set per identity.
 *
 * Responsibilities:
 * - Cap the in-flight requests of one identity at the ceiling its
 *   effective tier snapshot declares (zero = no ceiling)
 * - Nothing else: RPM/TPM buckets and the quota ledger belong to the
 *   other limiter and quota mechanisms; this gate only bounds how
 *   many requests one identity may have running at once
 *
 * The slots are process-local, matching the in-memory limiter's
 * deployment class: a single gateway instance is the honest scope.
 */
package limiter

import "sync"

// Concurrency is the per-tenant in-flight gate. It is safe for
// concurrent use.
type Concurrency struct {
	mu       sync.Mutex
	inFlight map[string]int64
}

// NewConcurrency builds an empty gate.
func NewConcurrency() *Concurrency {
	return &Concurrency{inFlight: make(map[string]int64)}
}

// Acquire takes one slot for the identity when its in-flight count is
// below max. The returned release function returns the slot and is
// idempotent. max at or below zero admits unconditionally — the
// ceiling is disabled.
func (c *Concurrency) Acquire(tenantID string, max int64) (release func(), ok bool) {
	if max <= 0 {
		return func() {}, true
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.inFlight[tenantID] >= max {
		return nil, false
	}
	c.inFlight[tenantID]++
	released := false
	return func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		if !released {
			released = true
			c.inFlight[tenantID]--
		}
	}, true
}

// InFlight reports the identity's current slot count, for tests and
// operations tooling.
func (c *Concurrency) InFlight(tenantID string) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.inFlight[tenantID]
}
