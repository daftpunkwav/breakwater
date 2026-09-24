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
 * Transient backend failures are never cached — only the definitive
 * ErrUnauthorized answer is, so a system-of-record outage degrades
 * request latency, not correctness.
 */
package auth

import (
	"container/list"
	"context"
	"errors"
	"sync"
	"time"
)

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
func NewCachedStore(inner Store, posTTL, negTTL time.Duration, opts ...CachedStoreOption) *CachedStore {
	c := &CachedStore{
		inner:  inner,
		items:  make(map[string]*list.Element),
		order:  list.New(),
		max:    4096,
		posTTL: posTTL,
		negTTL: negTTL,
		now:    time.Now,
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

// lookup returns the cached answer when it is fresh.
func (c *CachedStore) lookup(apiKey string) (Tenant, error, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	elem, ok := c.items[apiKey]
	if !ok {
		return Tenant{}, nil, false
	}
	entry := elem.Value.(*lruEntry)
	if c.now().After(entry.expires) {
		c.order.Remove(elem)
		delete(c.items, apiKey)
		return Tenant{}, nil, false
	}
	c.order.MoveToFront(elem)
	return entry.tenant, entry.err, true
}

// store inserts or refreshes an entry, evicting the LRU tail at cap.
func (c *CachedStore) store(apiKey string, entry lruEntry) {
	entry.key = apiKey
	if elem, ok := c.items[apiKey]; ok {
		cached := elem.Value.(*lruEntry)
		cached.tenant = entry.tenant
		cached.err = entry.err
		cached.expires = entry.expires
		c.order.MoveToFront(elem)
		return
	}
	c.items[apiKey] = c.order.PushFront(&entry)
	for c.order.Len() > c.max {
		tail := c.order.Back()
		if tail == nil {
			return
		}
		c.order.Remove(tail)
		delete(c.items, tail.Value.(*lruEntry).key)
	}
}
