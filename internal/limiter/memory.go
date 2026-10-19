/**
 * @file memory
 * @description The in-process limiter backend: continuous-refill token
 * buckets kept in memory.
 *
 * Responsibilities:
 * - The reference implementation of the limiter semantics: the same
 *   decisions the Redis backend reaches, with no serialization and no
 *   dependency on a shared clock. Stored state is not identical — a
 *   zero-capacity dimension still allocates a bucket here, while the
 *   Redis script skips writing one at all
 * - Serve tests and the in-memory degradation posture
 *
 * Buckets start full: the first request of a quiet tenant meets an
 * empty deficit, matching the Redis scripts. Refunds only ever raise a
 * bucket toward its capacity — a refund can never create allowance out
 * of thin air, it can only return what was reserved.
 *
 * Retention matches the Redis backend: a bucket idle for
 * bucketIdleTTL is dropped, which is the same horizon the Lua scripts
 * give their keys. The drop is invisible to the limiter's decisions —
 * see bucketIdleTTL for why.
 */
package limiter

import (
	"context"
	"math"
	"sync"
	"time"
)

// Memory is the in-process Limiter. It is safe for concurrent use.
type Memory struct {
	mu  sync.Mutex
	rpm map[string]*bucket
	tpm map[string]*bucket
	now func() time.Time
	// sweptAt is the earliest instant the next idle-bucket sweep may
	// run.
	sweptAt time.Time
}

type bucket struct {
	tokens float64
	last   time.Time
}

// bucketIdleTTL is how long a bucket may sit untouched before the
// sweep drops it. It is the PEXPIRE horizon of the Redis backend's two
// scripts (tokenbucket.lua and refund.lua), so the two implementations
// of this port retain their state on the same schedule.
//
// It is also the horizon at which dropping a bucket is provably
// indistinguishable from keeping one: a bucket's tokens never fall
// below zero (Allow only deducts what the bucket affords) and never
// exceed its capacity (refill and refund both clamp), while the refill
// rate is capacity tokens per 60s — so after a full idle horizon every
// bucket stands at exactly its capacity, which is precisely what a
// bucket recreated for the next request starts at. A zero-capacity
// dimension is exempt by construction: Allow skips its check entirely
// and Refund returns before touching it.
const bucketIdleTTL = 120 * time.Second

// NewMemory builds the in-process backend.
func NewMemory() *Memory {
	return &Memory{
		rpm: make(map[string]*bucket),
		tpm: make(map[string]*bucket),
		now: time.Now,
	}
}

// WithClock overrides the clock for tests.
func (m *Memory) WithClock(now func() time.Time) *Memory {
	m.now = now
	return m
}

// Allow implements Limiter: one request against the RPM bucket, the
// estimated tokens against the TPM bucket; both must fit.
func (m *Memory) Allow(_ context.Context, tenantID string, limits Limits, tokens int64) (Decision, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.now()

	// Idle buckets stop occupying memory here, at most one pass per
	// horizon: a bucket is dropped by the first sweep after its idle
	// TTL, so the maps hold the buckets of the last two horizons rather
	// than one per tenant the process ever served. The pass is O(map)
	// under the lock every request shares, which is why the interval is
	// the horizon rather than the sweepInterval cache.Memory uses — the
	// bound is only ever needed to be loose, not tight.
	if m.sweptAt.IsZero() || !m.sweptAt.After(now) {
		m.sweepLocked(now)
		m.sweptAt = now.Add(bucketIdleTTL)
	}

	rpm, tpm := m.refilled(tenantID, limits, now)

	rpmOK := limits.RPM <= 0 || rpm.tokens >= 1
	tpmOK := limits.TPM <= 0 || tpm.tokens >= float64(tokens)
	if rpmOK && tpmOK {
		if limits.RPM > 0 {
			rpm.tokens -= 1
		}
		if limits.TPM > 0 {
			tpm.tokens -= float64(tokens)
		}
		return Decision{Allowed: true}, nil
	}

	// The wait until the most deficient bucket can afford the request.
	var wait time.Duration
	if !rpmOK {
		wait = deficitWait(1-rpm.tokens, limits.RPM)
	}
	if !tpmOK {
		tpmWait := deficitWait(float64(tokens)-tpm.tokens, limits.TPM)
		if tpmWait > wait {
			wait = tpmWait
		}
	}
	return Decision{Allowed: false, RetryAfter: wait}, nil
}

// Refund implements Limiter: returns unconsumed tokens to the TPM
// bucket. The request itself stays consumed in RPM terms.
func (m *Memory) Refund(_ context.Context, tenantID string, limits Limits, tokens int64) error {
	if tokens <= 0 || limits.TPM <= 0 {
		return nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	_, tpm := m.refilled(tenantID, limits, m.now())
	tpm.tokens = min(tpm.tokens+float64(tokens), float64(limits.TPM))
	return nil
}

// refilled returns both buckets of a tenant, refilled to now. Buckets
// start full.
func (m *Memory) refilled(tenantID string, limits Limits, now time.Time) (*bucket, *bucket) {
	return m.refillOne(m.rpm, tenantID+":rpm", limits.RPM, now),
		m.refillOne(m.tpm, tenantID+":tpm", limits.TPM, now)
}

func (m *Memory) refillOne(buckets map[string]*bucket, key string, capacity int64, now time.Time) *bucket {
	b, ok := buckets[key]
	if !ok {
		b = &bucket{tokens: float64(capacity), last: now}
		buckets[key] = b
		return b
	}
	if capacity > 0 {
		rate := float64(capacity) / 60.0 // tokens per second
		b.tokens = min(float64(capacity), b.tokens+now.Sub(b.last).Seconds()*rate)
	}
	b.last = now
	return b
}

// sweepLocked drops every bucket idle for at least the horizon, the
// memory-side equivalent of the Redis scripts' PEXPIRE. Both maps are
// walked together: a tenant's RPM and TPM buckets are created and
// refreshed as a pair, so one pass over each costs no more than keeping
// them in step.
//
// Dropping is invisible to the decisions that follow — see
// bucketIdleTTL for the arithmetic — so the sweep only reclaims memory
// the buckets no longer influence.
func (m *Memory) sweepLocked(now time.Time) {
	cutoff := now.Add(-bucketIdleTTL)
	for key, b := range m.rpm {
		if !b.last.After(cutoff) {
			delete(m.rpm, key)
		}
	}
	for key, b := range m.tpm {
		if !b.last.After(cutoff) {
			delete(m.tpm, key)
		}
	}
}

// deficitWait converts a token deficit and capacity into the wait until
// a continuous refill covers it.
func deficitWait(deficit float64, capacity int64) time.Duration {
	if capacity <= 0 || deficit <= 0 {
		return 0
	}
	seconds := deficit / (float64(capacity) / 60.0)
	// Saturate before the conversion: a deficit far past what a Duration
	// can hold (an enormous request against a near-zero TPM) overflows
	// the float→Duration conversion into a negative, which the clamp
	// below would report as a 1ms wait — an invitation to hammer the
	// gateway. An effectively-unreachable refill is the honest answer.
	if seconds >= maxDurationSeconds {
		return time.Duration(math.MaxInt64)
	}
	wait := time.Duration(seconds * float64(time.Second))
	if wait < time.Millisecond {
		wait = time.Millisecond
	}
	return wait
}

// maxDurationSeconds is the largest wait, in seconds, that survives the
// float→Duration conversion without wrapping.
const maxDurationSeconds = math.MaxInt64 / float64(time.Second)
