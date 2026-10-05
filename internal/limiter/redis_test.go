/**
 * @file redis_test
 * @description Redis limiter backend tests over miniredis: script
 * semantics, parity with the in-memory backend and clock behavior.
 */
package limiter

import (
	"context"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestRedis(t *testing.T) (*Redis, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return NewRedis(client, ""), mr
}

// TestRetryAfterDurationSaturates: the script's millisecond wait
// survives the conversion intact in the normal range; a wait past the
// Duration range — the residue of an astronomical deficit — saturates
// at the ceiling instead of wrapping negative.
func TestRetryAfterDurationSaturates(t *testing.T) {
	t.Parallel()
	if got := retryAfterDuration(0); got != 0 {
		t.Fatalf("zero wait = %v, want 0", got)
	}
	if got := retryAfterDuration(1500); got != 1500*time.Millisecond {
		t.Fatalf("plain wait = %v, want 1.5s", got)
	}
	if got := retryAfterDuration(math.MaxInt64); got != time.Duration(math.MaxInt64) {
		t.Fatalf("huge wait = %v, want the Duration ceiling", got)
	}
	if got := retryAfterDuration(-5); got != time.Duration(math.MaxInt64) {
		t.Fatalf("wrapped wait = %v, want the Duration ceiling", got)
	}
}

func TestNamespacedKeyPrefix(t *testing.T) {
	t.Parallel()
	if got := namespacedKeyPrefix(""); got != defaultKeyPrefix {
		t.Fatalf("empty namespace prefix = %q, want %q", got, defaultKeyPrefix)
	}
	if got, want := namespacedKeyPrefix("env-a"), "env-a:"+defaultKeyPrefix; got != want {
		t.Fatalf("namespaced prefix = %q, want %q", got, want)
	}
}

func TestRedisNamespaceScopesKeys(t *testing.T) {
	t.Parallel()
	mr := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	r := NewRedis(client, "env-a")
	ctx := context.Background()

	if d, err := r.Allow(ctx, "t1", Limits{RPM: 1, TPM: 0}, 0); err != nil || !d.Allowed {
		t.Fatalf("allow: allowed=%v err=%v", d.Allowed, err)
	}
	// The namespace sits in front of the package prefix: two deployments
	// sharing one Redis must never throttle against each other's buckets.
	if !mr.Exists("env-a:bw:limiter:t1:rpm") {
		t.Fatalf("namespaced bucket missing; keys = %v", mr.Keys())
	}
	for _, k := range mr.Keys() {
		if strings.HasPrefix(k, defaultKeyPrefix) {
			t.Fatalf("bucket %q leaked outside the namespace", k)
		}
	}
}

func TestRedisAllowWithinLimits(t *testing.T) {
	t.Parallel()
	r, _ := newTestRedis(t)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		d, err := r.Allow(ctx, "t1", Limits{RPM: 3, TPM: 1000}, 10)
		if err != nil {
			t.Fatalf("allow %d: %v", i, err)
		}
		if !d.Allowed {
			t.Fatalf("request %d rejected within limits", i)
		}
	}
	d, err := r.Allow(ctx, "t1", Limits{RPM: 3, TPM: 1000}, 10)
	if err != nil {
		t.Fatalf("allow 4: %v", err)
	}
	if d.Allowed {
		t.Fatal("request 4 allowed beyond RPM")
	}
	if d.RetryAfter <= 0 {
		t.Fatalf("retry after = %v, want positive", d.RetryAfter)
	}
}

func TestRedisTPMReserveAndRefund(t *testing.T) {
	t.Parallel()
	r, _ := newTestRedis(t)
	ctx := context.Background()

	limits := Limits{RPM: 0, TPM: 100}
	d, err := r.Allow(ctx, "t1", limits, 60)
	if err != nil || !d.Allowed {
		t.Fatalf("first allow: %v %v", d.Allowed, err)
	}
	// Only 40 tokens of headroom left.
	if d, err = r.Allow(ctx, "t1", limits, 50); err != nil || d.Allowed {
		t.Fatalf("oversized reservation allowed: %v %v", d.Allowed, err)
	}
	// The unconsumed reservation goes back: 40 + 60 = full capacity.
	if err := r.Refund(ctx, "t1", limits, 60); err != nil {
		t.Fatalf("refund: %v", err)
	}
	if d, err = r.Allow(ctx, "t1", limits, 100); err != nil || !d.Allowed {
		t.Fatalf("full-capacity allow after refund: %v %v", d.Allowed, err)
	}
}

func TestRedisZeroLimitDisablesCeiling(t *testing.T) {
	t.Parallel()
	r, _ := newTestRedis(t)
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		d, err := r.Allow(ctx, "t1", Limits{RPM: 0, TPM: 0}, 1<<20)
		if err != nil || !d.Allowed {
			t.Fatalf("request %d: allowed=%v err=%v", i, d.Allowed, err)
		}
	}
}

func TestRedisInsufficientBalanceIsPerTenant(t *testing.T) {
	t.Parallel()
	r, _ := newTestRedis(t)
	ctx := context.Background()
	limits := Limits{RPM: 1, TPM: 0}

	if d, err := r.Allow(ctx, "a", limits, 0); err != nil || !d.Allowed {
		t.Fatalf("tenant a: %v %v", d.Allowed, err)
	}
	if d, err := r.Allow(ctx, "a", limits, 0); err != nil || d.Allowed {
		t.Fatalf("tenant a second: %v %v", d.Allowed, err)
	}
	if d, err := r.Allow(ctx, "b", limits, 0); err != nil || !d.Allowed {
		t.Fatalf("tenant b must be independent: %v %v", d.Allowed, err)
	}
}

func TestRedisRefillOverTime(t *testing.T) {
	t.Parallel()
	r, mr := newTestRedis(t)
	ctx := context.Background()
	limits := Limits{RPM: 60, TPM: 0} // capacity 60, refill 1 token/s

	// miniredis's TIME follows SetTime; keep a moving clock.
	clock := time.Now()
	mr.SetTime(clock)

	// Drain the whole burst.
	drainBurst(t, ctx, r, "t", limits, 60, "burst")
	mustReject(t, ctx, r, "t", limits, "post-burst must reject")
	// Two seconds refill two tokens: two requests pass, the third waits.
	clock = clock.Add(2 * time.Second)
	mr.SetTime(clock)
	drainBurst(t, ctx, r, "t", limits, 2, "refill")
	mustReject(t, ctx, r, "t", limits, "beyond refill must reject")
	// And the burst refills fully over the minute.
	clock = clock.Add(time.Minute)
	mr.SetTime(clock)
	drainBurst(t, ctx, r, "t", limits, 60, "full-refill")
}
