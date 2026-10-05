/**
 * @file memory
 * @description The in-process exact-match response cache.
 *
 * Responsibilities:
 * - Store entries under content-hash keys with expiry
 * - Apply the expiry jitter on Set so aligned TTLs cannot expire into
 *   a stampede
 * - Reclaim expired entries on a throttled in-write sweep, so time
 *   cannot quietly eat the capacity that the admission gate defends
 * - Admit new entries under capacity pressure through a TinyLFU-style
 *   gate: the newcomer must be at least as frequently read as the
 *   sampled victim it would displace (see admission.go)
 * - Preserve entry ownership: every Get returns private copies, never
 *   shared state (entries are replayed to concurrent clients)
 *
 * The cache is an optimization, never a correctness dependency: a
 * missing or failed backend degrades to direct upstream forwarding.
 */
package cache

import (
	"context"
	"math"
	// nosemgrep: go.lang.security.audit.crypto.math_random.math-random-used -- this file draws only non-cryptographic TTL jitter; no key material comes from it
	"math/rand/v2"
	"sync"
	"time"
)

// sweepInterval paces the expired-entry sweep: at most one full scan
// per interval, triggered from the write path. A thousand-entry scan
// costs microseconds; the per-Set check is one timestamp comparison.
const sweepInterval = time.Second

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
	// sweepAt is the earliest instant the next expiry sweep may run.
	sweepAt time.Time
	// jitterFraction bounds the random TTL spread: effective TTL is
	// base * (1 ± jitterFraction).
	jitterFraction float64
	// gate tracks read frequency and decides which entry leaves under
	// capacity pressure. It keeps its own lock: reads feed it from the
	// cache's RLock path.
	gate *admission
}

// MemoryOption customizes a Memory cache.
type MemoryOption func(*Memory)

// WithCapacity caps the entry count. Under pressure the gate decides:
// a new entry is admitted only when its read frequency beats the
// sampled victim it would displace, so a scan of one-shot prompts
// cannot wash out the entries callers keep asking for. The cap is an
// entry count, not a byte budget: one entry may hold up to
// maxCacheableBytes (see middleware.go), so the worst-case footprint
// is capacity times that bound.
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
	// The gate tracks roughly eight times the capacity in keys: small
	// enough to stay in tens of kilobytes, large enough that the
	// sketch's false-positive floor stays far below the doorkeeper's.
	m.gate = newAdmission(m.max * 8)
	return m
}

// Get implements Cache.
func (m *Memory) Get(_ context.Context, key string) (Entry, error) {
	m.mu.RLock()
	me, ok := m.entries[key]
	m.mu.RUnlock()
	// Every read feeds the admission gate — misses as much as hits: a
	// rejected key keeps earning its way in while callers keep asking
	// for it.
	m.gate.record(m.gate.hash(key))
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
	// The multiply runs in float space; a base TTL near the Duration
	// ceiling overflows the conversion into a negative, which the clamp
	// below would store as a 1ms entry — silently defeating the
	// configured TTL. Saturate instead of wrapping.
	scaled := float64(baseTTL) * jitter
	ttl := time.Duration(scaled)
	if scaled >= float64(math.MaxInt64) {
		ttl = time.Duration(math.MaxInt64)
	} else if ttl < time.Millisecond {
		ttl = time.Millisecond
	}
	// Clone before taking the write lock: the private copy is the
	// store's ownership boundary, and holding the write lock across a
	// copy of up to the capture cap would serialize every concurrent
	// Set behind it.
	stored := Entry{
		Status: entry.Status,
		Header: entry.Header.Clone(),
		Body:   append([]byte(nil), entry.Body...),
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	// Expired entries stop occupying capacity here: a sweep at most
	// once per interval, from the write path that already holds the
	// lock. Without it, time would quietly eat the slots the admission
	// gate defends, and the gate's victim samples would skew toward
	// the dead.
	now := m.now()
	if m.sweepAt.IsZero() || !m.sweepAt.After(now) {
		m.sweepLocked(now)
		m.sweepAt = now.Add(sweepInterval)
	}
	// An update of a resident key bypasses the gate: refreshing what
	// callers demonstrably use is always right.
	if _, resident := m.entries[key]; !resident && len(m.entries) >= m.max {
		if !m.admitLocked(key) {
			// The newcomer lost to a more-read resident. Dropping it is
			// the gate working: the copy dies here, nothing is stored.
			return nil
		}
	}
	m.entries[key] = memoryEntry{
		entry:   stored,
		expires: now.Add(ttl),
	}
	return nil
}

// sweepLocked deletes every expired entry. The map walk is bounded by
// the capacity and runs at most once per sweepInterval; visibility is
// unaffected either way, because Get judges expiry itself — this only
// frees the slots earlier than overwrite would.
func (m *Memory) sweepLocked(now time.Time) {
	for key, me := range m.entries {
		if !now.Before(me.expires) {
			delete(m.entries, key)
		}
	}
}

// admitLocked runs the capacity-pressure decision: a random sample of
// residents picks the eviction victim (the least-read one), and the
// newcomer is admitted only when its estimate matches or beats the
// victim's. Reports whether the newcomer wins a slot.
func (m *Memory) admitLocked(key string) bool {
	h := m.gate.hash(key)
	incoming := m.gate.estimate(h)

	// Sampled-LFU eviction: the victim is the least-read entry among a
	// random sample of residents, and the newcomer must match or beat
	// its estimate to take the slot (a tie admits — rejecting ties
	// would bias the cache toward ancient entries).
	victim := ""
	victimEstimate := -1
	sample := 5
	for candidate := range m.entries {
		if est := m.gate.estimate(m.gate.hash(candidate)); victimEstimate < 0 || est < victimEstimate {
			victim, victimEstimate = candidate, est
		}
		sample--
		if sample == 0 {
			break
		}
	}
	if incoming < victimEstimate {
		return false
	}
	delete(m.entries, victim)
	return true
}
