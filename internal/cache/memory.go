/**
 * @file memory
 * @description The in-process exact-match response cache.
 *
 * Responsibilities:
 * - Store entries under content-hash keys with expiry
 * - Apply the expiry jitter on Set so aligned TTLs cannot expire into
 *   a stampede
 * - Preserve entry ownership: every Get returns private copies, never
 *   shared state (entries are replayed to concurrent clients)
 *
 * The cache is an optimization, never a correctness dependency: a
 * missing or failed backend degrades to direct upstream forwarding.
 */
package cache

import (
	"context"
	"math/rand/v2"
	"sync"
	"time"
)

// memoryEntry is the stored form; expiry is absolute.
type memoryEntry struct {
	entry   Entry
	expires time.Time
}

// Memory is the in-process Cache. It is safe for concurrent use.
type Memory struct {
	mu      sync.RWMutex
	entries map[string]memoryEntry
	max     int
	now     func() time.Time
	// jitterFraction bounds the random TTL spread: effective TTL is
	// base * (1 ± jitterFraction).
	jitterFraction float64
}

// MemoryOption customizes a Memory cache.
type MemoryOption func(*Memory)

// WithCapacity caps the entry count; the insertion-evicted victim is
// an arbitrary expired-or-oldest entry.
func WithCapacity(n int) MemoryOption {
	return func(m *Memory) {
		if n >= 1 {
			m.max = n
		}
	}
}

// WithClock overrides the clock for tests.
func WithClock(now func() time.Time) MemoryOption {
	return func(m *Memory) { m.now = now }
}

// NewMemory builds the in-process cache.
func NewMemory(opts ...MemoryOption) *Memory {
	m := &Memory{
		entries:        make(map[string]memoryEntry),
		max:            1024,
		now:            time.Now,
		jitterFraction: 0.1,
	}
	for _, opt := range opts {
		opt(m)
	}
	return m
}

// Get implements Cache.
func (m *Memory) Get(_ context.Context, key string) (Entry, error) {
	m.mu.RLock()
	me, ok := m.entries[key]
	m.mu.RUnlock()
	if !ok || !m.now().Before(me.expires) {
		return Entry{}, ErrMiss
	}
	// Ownership: private copies per Get.
	return Entry{
		Status: me.entry.Status,
		Header: me.entry.Header.Clone(),
		Body:   append([]byte(nil), me.entry.Body...),
	}, nil
}

// Set implements Cache: the base TTL is widened by a random jitter so
// expirations do not align into a storm.
func (m *Memory) Set(_ context.Context, key string, entry Entry, baseTTL time.Duration) error {
	if baseTTL <= 0 {
		return nil
	}
	jitter := 1 + (2*rand.Float64()-1)*m.jitterFraction
	ttl := time.Duration(float64(baseTTL) * jitter)
	if ttl < time.Millisecond {
		ttl = time.Millisecond
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.entries) >= m.max {
		m.evictOne()
	}
	m.entries[key] = memoryEntry{
		entry: Entry{
			Status: entry.Status,
			Header: entry.Header.Clone(),
			Body:   append([]byte(nil), entry.Body...),
		},
		expires: m.now().Add(ttl),
	}
	return nil
}

// evictOne drops an arbitrary entry (map iteration order): capacity
// pressure is a size heuristic, not a policy.
func (m *Memory) evictOne() {
	for key := range m.entries {
		delete(m.entries, key)
		return
	}
}
