/**
 * @file redis_unavailable_test
 * @description The Redis backend's guard rails and its behavior when
 * the backend is unreachable: health reporting, script failure
 * surfacing and the refund no-ops.
 */
package limiter

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestRedisPingReportsHealth(t *testing.T) {
	t.Parallel()
	r, mr := newTestRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := r.Ping(ctx); err != nil {
		t.Fatalf("ping against a live server: %v", err)
	}
	mr.Close()
	if err := r.Ping(ctx); err == nil {
		t.Fatal("ping against a closed server must report unhealthy")
	}
}

func TestRedisAllowSurfacesBackendFailure(t *testing.T) {
	t.Parallel()
	r, mr := newTestRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mr.Close()

	if _, err := r.Allow(ctx, "t", Limits{RPM: 10, TPM: 1000}, 10); err == nil ||
		!strings.Contains(err.Error(), "allow script") {
		t.Fatalf("err = %v, want the wrapped allow script failure", err)
	}
}

func TestRedisRefundGuardsAndFailure(t *testing.T) {
	t.Parallel()
	r, mr := newTestRedis(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Zero tokens and a disabled TPM ceiling never touch the backend.
	if err := r.Refund(ctx, "t", Limits{RPM: 0, TPM: 100}, 0); err != nil {
		t.Fatalf("zero refund: %v", err)
	}
	if err := r.Refund(ctx, "t", Limits{RPM: 10, TPM: 0}, 50); err != nil {
		t.Fatalf("refund with disabled TPM: %v", err)
	}

	mr.Close()
	if err := r.Refund(ctx, "t", Limits{RPM: 0, TPM: 100}, 10); err == nil ||
		!strings.Contains(err.Error(), "refund script") {
		t.Fatalf("err = %v, want the wrapped refund script failure", err)
	}
}
