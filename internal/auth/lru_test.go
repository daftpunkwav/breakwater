/**
 * @file lru_test
 * @description Identity cache tests: positive and negative caching,
 * TTL expiry, LRU eviction, and the rule that transient backend
 * failures are never cached.
 */
package auth

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// fakeStore counts resolutions and answers from a function.
type fakeStore struct {
	resolves int
	fn       func(apiKey string) (Tenant, error)
}

func (s *fakeStore) Resolve(_ context.Context, apiKey string) (Tenant, error) {
	s.resolves++
	return s.fn(apiKey)
}

func testTenant(id string) Tenant {
	return Tenant{ID: id, Name: id, Tier: Tier{ID: "free", RPM: 10, TPM: 1000}}
}

func TestCachedStorePositiveCaching(t *testing.T) {
	t.Parallel()
	inner := &fakeStore{fn: func(string) (Tenant, error) { return testTenant("t1"), nil }}
	cached := NewCachedStore(inner, time.Minute, time.Second)

	for i := 0; i < 5; i++ {
		tenant, err := cached.Resolve(context.Background(), "key")
		if err != nil || tenant.ID != "t1" {
			t.Fatalf("resolve %d: %v %v", i, tenant, err)
		}
	}
	if inner.resolves != 1 {
		t.Fatalf("backend resolved %d times, want 1 (hot path stays local)", inner.resolves)
	}
}

func TestCachedStoreNegativeCaching(t *testing.T) {
	t.Parallel()
	inner := &fakeStore{fn: func(string) (Tenant, error) { return Tenant{}, ErrUnauthorized }}
	cached := NewCachedStore(inner, time.Minute, time.Second)

	for i := 0; i < 5; i++ {
		if _, err := cached.Resolve(context.Background(), "bogus"); !errors.Is(err, ErrUnauthorized) {
			t.Fatalf("resolve %d err = %v", i, err)
		}
	}
	if inner.resolves != 1 {
		t.Fatalf("backend resolved %d times, want 1 (negatives cached)", inner.resolves)
	}
}

func TestCachedStoreTTLExpiry(t *testing.T) {
	t.Parallel()
	inner := &fakeStore{fn: func(string) (Tenant, error) { return testTenant("t1"), nil }}
	now := time.Now()
	cached := NewCachedStore(inner, time.Minute, time.Second, WithCacheClock(func() time.Time { return now }))

	if _, err := cached.Resolve(context.Background(), "key"); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	now = now.Add(2 * time.Minute)
	if _, err := cached.Resolve(context.Background(), "key"); err != nil {
		t.Fatalf("resolve after expiry: %v", err)
	}
	if inner.resolves != 2 {
		t.Fatalf("backend resolved %d times, want 2 after TTL expiry", inner.resolves)
	}
}

func TestCachedStoreEvictsLeastRecentlyUsed(t *testing.T) {
	t.Parallel()
	inner := &fakeStore{fn: func(key string) (Tenant, error) { return testTenant(key), nil }}
	cached := NewCachedStore(inner, time.Minute, time.Second, WithCacheCapacity(2))

	ctx := context.Background()
	for _, key := range []string{"a", "b"} {
		if _, err := cached.Resolve(ctx, key); err != nil {
			t.Fatalf("resolve %s: %v", key, err)
		}
	}
	// Refresh a, making b the LRU tail.
	if _, err := cached.Resolve(ctx, "a"); err != nil {
		t.Fatalf("re-resolve a: %v", err)
	}
	if _, err := cached.Resolve(ctx, "c"); err != nil {
		t.Fatalf("resolve c: %v", err)
	}
	// b was evicted: resolving it hits the backend again.
	if _, err := cached.Resolve(ctx, "b"); err != nil {
		t.Fatalf("resolve b: %v", err)
	}
	if inner.resolves != 4 {
		t.Fatalf("backend resolved %d times, want 4 (a,b,a-miss-free? c evicts b, b again)", inner.resolves)
	}
}

func TestCachedStoreTransientErrorsNeverCached(t *testing.T) {
	t.Parallel()
	calls := 0
	inner := &fakeStore{fn: func(string) (Tenant, error) {
		calls++
		if calls == 1 {
			return Tenant{}, fmt.Errorf("identity database unreachable")
		}
		return testTenant("t1"), nil
	}}
	cached := NewCachedStore(inner, time.Minute, time.Second)
	ctx := context.Background()

	if _, err := cached.Resolve(ctx, "key"); err == nil {
		t.Fatal("first resolve must surface the backend failure")
	}
	tenant, err := cached.Resolve(ctx, "key")
	if err != nil || tenant.ID != "t1" {
		t.Fatalf("second resolve: %v %v", tenant, err)
	}
	if calls != 2 {
		t.Fatalf("backend resolved %d times, want 2: failures must not poison the cache", calls)
	}
}
