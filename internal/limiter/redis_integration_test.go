/**
 * @file redis_integration_test
 * @description Smoke test against a real Redis: script execution with
 * a live server clock. Skipped unless BREAKWATER_TEST_REDIS_ADDR is
 * set (the CI workflow provides it with a service container).
 */
package limiter

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRedisLiveSmoke(t *testing.T) {
	addr := os.Getenv("BREAKWATER_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("integration: BREAKWATER_TEST_REDIS_ADDR not set")
	}
	client := redis.NewClient(&redis.Options{Addr: addr})
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	r := NewRedis(client)
	limits := Limits{RPM: 1000, TPM: 100_000}
	if d, err := r.Allow(ctx, "smoke", limits, 10); err != nil || !d.Allowed {
		t.Fatalf("allow: %v %v", d.Allowed, err)
	}
	if err := r.Refund(ctx, "smoke", limits, 5); err != nil {
		t.Fatalf("refund: %v", err)
	}
}
