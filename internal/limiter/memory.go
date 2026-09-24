/**
 * @file memory
 * @description The in-process limiter backend: continuous-refill token
 * buckets kept in memory.
 *
 * Responsibilities:
 * - The reference implementation of the limiter semantics: identical
 *   behavior to the Redis backend, no serialization, no clock skew
 * - Serve tests and the in-memory degradation posture
 *
 * Buckets start full: the first request of a quiet tenant meets an
 * empty deficit, matching the Redis scripts. Refunds only ever raise a
 * bucket toward its capacity — a refund can never create allowance out
 * of thin air, it can only return what was reserved.
 */
package limiter

import (
	"context"
	"sync"
	"time"
)

// Memory is the in-process Limiter. It is safe for concurrent use.
type Memory struct {
	mu  sync.Mutex
	rpm map[string]*bucket
	tpm map[string]*bucket
	now func() time.Time
}

type bucket struct {
	tokens float64
	last   time.Time
}

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

// deficitWait converts a token deficit and capacity into the wait until
// a continuous refill covers it.
func deficitWait(deficit float64, capacity int64) time.Duration {
	if capacity <= 0 || deficit <= 0 {
		return 0
	}
	seconds := deficit / (float64(capacity) / 60.0)
	wait := time.Duration(seconds * float64(time.Second))
	if wait < time.Millisecond {
		wait = time.Millisecond
	}
	return wait
}
