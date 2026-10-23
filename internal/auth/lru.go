/**
 * @file lru
 * @description The process-local identity cache: an LRU with TTL,
 * wrapping the system-of-record store.
 *
 * Responsibilities:
 * - Keep the steady-state authentication path off distributed I/O:
 *   hot keys resolve from process memory
 * - Cache negatives briefly, so a flood of bogus keys does not reach
 *   the system of record
 * - Nothing else: revocation latency equals the positive TTL and is
 *   part of the public contract; nothing here validates keys
 *
 * The cache indexes by a keyed digest of the raw API key, never by the
 * key itself: the lookup map holds no usable key, so a leak of it
 * yields nothing.
 *
 * Transient backend failures are never cached — only the definitive
 * ErrUnauthorized answer is, so a system-of-record outage degrades
 * request latency, not correctness.
 */
package auth

import (
	"container/list"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"sync"
	"time"
)

// cacheIndexKeyLen is the length of the per-process key material that
// digests the raw API keys the cache indexes by.
const cacheIndexKeyLen = 32

// lruEntry is one cached resolution. Errors are cached only when
// definitive (ErrUnauthorized); transient failures skip the cache.
type lruEntry struct {
	key     string
	tenant  Tenant
	err     error
	expires time.Time
}

// CachedStore decorates a Store with the process-local LRU.
type CachedStore struct {
	inner  Store
	mu     sync.Mutex
	items  map[string]*list.Element
	order  *list.List // front = most recently used
	max    int
	posTTL time.Duration
	negTTL time.Duration
	now    func() time.Time
	// indexKey is the per-process secret the index digest is keyed
	// with. It never leaves the process, so a digest lifted from the
	// lookup map cannot be turned back into a raw API key.
	indexKey string
}

// CachedStoreOption customizes a CachedStore.
type CachedStoreOption func(*CachedStore)

// WithCacheCapacity caps the cache; least recently used entries evict
// first. Defaults to 4096 keys.
func WithCacheCapacity(n int) CachedStoreOption {
	return func(c *CachedStore) {
		if n >= 1 {
			c.max = n
		}
	}
}

// WithCacheClock overrides the clock for tests.
func WithCacheClock(now func() time.Time) CachedStoreOption {
	return func(c *CachedStore) { c.now = now }
}

// NewCachedStore wraps inner with an LRU cache. Positive resolutions
// live for posTTL, unauthorized negatives for negTTL (shorter: bots
// rotate keys faster than tenants revoke them).
//
// The cache indexes by a digest of the raw API key, keyed with a
// secret generated for this store: the lookup map holds no usable key,
// so a leak of it yields nothing.
func NewCachedStore(inner Store, posTTL, negTTL time.Duration, opts ...CachedStoreOption) *CachedStore {
	var indexKey [cacheIndexKeyLen]byte
	if _, err := rand.Read(indexKey[:]); err != nil {
		panic(fmt.Sprintf("auth: cache index entropy unavailable: %v", err))
	}
	c := &CachedStore{
		inner:    inner,
		items:    make(map[string]*list.Element),
		order:    list.New(),
		max:      4096,
		posTTL:   posTTL,
		negTTL:   negTTL,
		now:      time.Now,
		indexKey: string(indexKey[:]),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// Resolve implements Store: memory first, system of record on miss.
func (c *CachedStore) Resolve(ctx context.Context, apiKey string) (Tenant, error) {
	if tenant, err, ok := c.lookup(apiKey); ok {
		return tenant, err
	}
	tenant, err := c.inner.Resolve(ctx, apiKey)

	c.mu.Lock()
	defer c.mu.Unlock()
	switch {
	case err == nil:
		c.store(apiKey, lruEntry{tenant: tenant, expires: c.now().Add(c.posTTL)})
	case errors.Is(err, ErrUnauthorized):
		c.store(apiKey, lruEntry{err: ErrUnauthorized, expires: c.now().Add(c.negTTL)})
	default:
		// Transient backend failure: surface it, cache nothing.
	}
	return tenant, err
}

// lookup returns the cached answer when it is fresh. The returned
// Tenant shares its Tier slice/map fields (AllowedModels, DeniedModels,
// ModelQuotas) with the cached entry: every current caller treats the
// snapshot as read-only, so no copy is made. A future caller that
// mutates the snapshot must shallow-copy those fields here first, or it
// will corrupt every other request resolving through the same entry.
func (c *CachedStore) lookup(apiKey string) (Tenant, error, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := c.index(apiKey)
	elem, ok := c.items[key]
	if !ok {
		return Tenant{}, nil, false
	}
	entry := elem.Value.(*lruEntry)
	if c.now().After(entry.expires) {
		c.order.Remove(elem)
		delete(c.items, key)
		return Tenant{}, nil, false
	}
	c.order.MoveToFront(elem)
	return entry.tenant, entry.err, true
}

// store inserts or refreshes an entry, evicting the LRU tail at cap.
func (c *CachedStore) store(apiKey string, entry lruEntry) {
	key := c.index(apiKey)
	entry.key = key
	if elem, ok := c.items[key]; ok {
		cached := elem.Value.(*lruEntry)
		cached.tenant = entry.tenant
		cached.err = entry.err
		cached.expires = entry.expires
		c.order.MoveToFront(elem)
		return
	}
	c.items[key] = c.order.PushFront(&entry)
	for c.order.Len() > c.max {
		tail := c.order.Back()
		if tail == nil {
			return
		}
		c.order.Remove(tail)
		delete(c.items, tail.Value.(*lruEntry).key)
	}
}

// index returns the map key for a raw API key: a digest keyed with
// the store's own secret. The raw key is never retained, so the
// lookup map holds no usable key.
func (c *CachedStore) index(apiKey string) string {
	return hashKey(c.indexKey, apiKey)
}
