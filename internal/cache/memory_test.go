/**
 * @file memory_test
 * @description In-memory cache tests: expiry, jitter bounds, entry
 * ownership (private copies per Get) and capacity.
 */
package cache

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

func testEntry() Entry {
	header := http.Header{}
	header.Set("Content-Type", "application/json")
	return Entry{Status: 200, Header: header, Body: []byte("hello")}
}

func TestMemorySetGet(t *testing.T) {
	t.Parallel()
	m := NewMemory()
	ctx := context.Background()

	if _, err := m.Get(ctx, "missing"); err != ErrMiss {
		t.Fatalf("err = %v, want ErrMiss", err)
	}
	if err := m.Set(ctx, "k", testEntry(), time.Minute); err != nil {
		t.Fatalf("set: %v", err)
	}
	entry, err := m.Get(ctx, "k")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if entry.Status != 200 || string(entry.Body) != "hello" {
		t.Fatalf("entry = %+v", entry)
	}
	if entry.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("header lost: %v", entry.Header)
	}
}

func TestMemoryExpiry(t *testing.T) {
	t.Parallel()
	var now atomic.Value
	now.Store(time.Now())
	m := NewMemory(WithClock(func() time.Time { return now.Load().(time.Time) }))
	ctx := context.Background()

	_ = m.Set(ctx, "k", testEntry(), time.Minute)
	now.Store(now.Load().(time.Time).Add(2 * time.Minute))
	if _, err := m.Get(ctx, "k"); err != ErrMiss {
		t.Fatalf("err = %v, want ErrMiss after expiry", err)
	}
}

// TestMemoryOwnership pins the frozen contract: mutating a returned
// entry must never corrupt the stored copy.
func TestMemoryOwnership(t *testing.T) {
	t.Parallel()
	m := NewMemory()
	ctx := context.Background()
	_ = m.Set(ctx, "k", testEntry(), time.Minute)

	first, _ := m.Get(ctx, "k")
	first.Body[0] = 'X'
	first.Header.Set("Content-Type", "text/html")

	second, err := m.Get(ctx, "k")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if second.Body[0] != 'h' {
		t.Fatalf("stored body mutated through a returned copy: %q", second.Body)
	}
	if second.Header.Get("Content-Type") != "application/json" {
		t.Fatalf("stored header mutated through a returned copy")
	}
}

func TestMemoryJitterKeepsTTLWithinBounds(t *testing.T) {
	t.Parallel()
	m := NewMemory()
	ctx := context.Background()

	// Store many entries and verify all live within [0.85, 1.15]x base
	// TTL plus a safety margin.
	base := time.Minute
	for i := range 200 {
		_ = m.Set(ctx, string(rune('a'+i%26))+string(rune('a'+i/26)), testEntry(), base)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	for _, me := range m.entries {
		age := me.expires.Sub(now)
		if age < time.Duration(0.8*float64(base)) || age > time.Duration(1.2*float64(base)) {
			t.Fatalf("TTL %v outside the jitter bounds of %v", age, base)
		}
	}
}

func TestMemoryCapacityEvicts(t *testing.T) {
	t.Parallel()
	m := NewMemory(WithCapacity(2))
	ctx := context.Background()

	_ = m.Set(ctx, "a", testEntry(), time.Minute)
	_ = m.Set(ctx, "b", testEntry(), time.Minute)
	_ = m.Set(ctx, "c", testEntry(), time.Minute)

	present := 0
	for _, key := range []string{"a", "b", "c"} {
		if _, err := m.Get(ctx, key); err == nil {
			present++
		}
	}
	if present != 2 {
		t.Fatalf("entries present = %d, want 2 after eviction", present)
	}
}
