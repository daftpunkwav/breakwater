/**
 * @file credential
 * @description The credential ring: several provider credentials
 * served by one upstream, rotated per exchange.
 *
 * Responsibilities:
 * - Hold the ordered credential list behind one adapter and pick the
 *   next eligible credential for each exchange
 * - Track per-credential retirement: a credential a completed exchange
 *   proved broken stops serving until the ring is revived
 * - Nothing else: which exchange convicts which credential is the
 *   caller's verdict (the relay's fatal classification); the ring only
 *   keeps the state
 *
 * Rotation is a round-robin cursor, not a per-request decision: the
 * cursor advances on every pick, so concurrent requests spread across
 * the ring. A request that must not reuse a credential passes the
 * exclusion list along with the exchange request.
 */
package upstream

import "sync/atomic"

// CredentialPool is the multi-credential face of an adapter: an
// upstream that rotates several provider credentials behind one id.
// The relay consults liveness to decide whether a fatal-classified
// exchange ends the request or hands it to the next credential; the
// assembly retires and revives through the same port.
type CredentialPool interface {
	// AliveCredentials reports how many credentials can currently
	// serve.
	AliveCredentials() int
	// RetireCredential marks one credential dead: no exchange uses it
	// again until the ring is revived. It reports whether this call
	// did the retiring — calls on an already-dead or unknown index
	// report false.
	RetireCredential(index int) bool
	// ReviveCredentials clears every retirement, restoring the full
	// ring. It is the downstream half of the upstream re-enable story:
	// whatever returns the upstream to rotation returns its
	// credentials with it.
	ReviveCredentials()
}

// credentialRing is the rotation state behind one adapter: an ordered,
// immutable credential list, one retirement flag per entry, and a
// round-robin cursor.
type credentialRing struct {
	keys   []string
	dead   []atomic.Bool
	cursor atomic.Uint64
}

func newCredentialRing(keys []string) *credentialRing {
	return &credentialRing{
		keys: keys,
		dead: make([]atomic.Bool, len(keys)),
	}
}

// pick returns the index of the next credential eligible to serve:
// alive and not excluded by this request. The scan starts at the ring
// cursor and leaves it just past the credential it hands out, so
// consecutive picks — across concurrent requests and across one
// request's exclusion walk — never repeat a credential before the
// ring has been walked once. When every alive credential is excluded
// the exclusion is dropped and the scan takes the first alive entry —
// one request must not starve behind its own walk. -1 reports that no
// credential is alive.
func (r *credentialRing) pick(excluded []int) int {
	n := len(r.keys)
	if n == 0 {
		return -1
	}
	start := int(r.cursor.Add(1)-1) % n
	for i := 0; i < n; i++ {
		idx := (start + i) % n
		if r.dead[idx].Load() || containsIndex(excluded, idx) {
			continue
		}
		r.cursor.Store(uint64(idx + 1))
		return idx
	}
	for i := 0; i < n; i++ {
		idx := (start + i) % n
		if !r.dead[idx].Load() {
			r.cursor.Store(uint64(idx + 1))
			return idx
		}
	}
	return -1
}

// retire marks the index dead and reports whether this call did the
// retiring. Unknown indexes are ignored.
func (r *credentialRing) retire(index int) bool {
	if index < 0 || index >= len(r.dead) {
		return false
	}
	return !r.dead[index].Swap(true)
}

// alive counts the credentials still eligible to serve.
func (r *credentialRing) alive() int {
	count := 0
	for i := range r.dead {
		if !r.dead[i].Load() {
			count++
		}
	}
	return count
}

// revive clears every retirement.
func (r *credentialRing) revive() {
	for i := range r.dead {
		r.dead[i].Store(false)
	}
}

// containsIndex reports whether the exclusion list names the index.
func containsIndex(excluded []int, index int) bool {
	for _, got := range excluded {
		if got == index {
			return true
		}
	}
	return false
}
