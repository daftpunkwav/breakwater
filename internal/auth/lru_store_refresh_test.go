/**
 * @file lru_store_refresh_test
 * @description The cache fill refresh: a key that is already stored
 * (two concurrent misses both filling) is updated in place instead of
 * duplicating the list entry.
 */
package auth

import (
	"context"
	"testing"
	"time"
)

// TestStoreRefreshUpdatesInPlace: two fills for one key converge —
// the stored snapshot is replaced, the recency is refreshed and no
// duplicate entry piles up.
func TestStoreRefreshUpdatesInPlace(t *testing.T) {
	t.Parallel()

	// The inner store must never be consulted: both fills happen
	// before any resolve, the race double-fill shape.
	inner := &fakeStore{fn: func(string) (Tenant, error) {
		t.Error("backend resolved while the answer was cached")
		return Tenant{}, nil
	}}
	cached := NewCachedStore(inner, time.Minute, time.Second)

	far := time.Now().Add(time.Minute)
	cached.store("key", lruEntry{tenant: testTenant("first"), expires: far})
	cached.store("key", lruEntry{tenant: testTenant("second"), expires: far})

	if got := len(cached.items); got != 1 {
		t.Fatalf("cached items = %d, want 1 (no duplicate entry)", got)
	}
	if got := cached.order.Len(); got != 1 {
		t.Fatalf("recency list length = %d, want 1", got)
	}

	tenant, err := cached.Resolve(context.Background(), "key")
	if err != nil || tenant.ID != "second" {
		t.Fatalf("Resolve = (%+v, %v), want the refreshed snapshot second", tenant, err)
	}
}
