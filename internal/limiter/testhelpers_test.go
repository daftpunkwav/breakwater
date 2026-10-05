/**
 * @file testhelpers_test
 * @description Shared limiter-behavior assertions used by both the
 * in-memory and the Redis backend tests: burst drains that must pass
 * and single requests that must be denied.
 */
package limiter

import (
	"context"
	"testing"
)

// drainBurst sends n requests that must all be allowed, failing at the
// first rejection or backend error. label names the phase in the
// failure message.
func drainBurst(t *testing.T, ctx context.Context, l Limiter, tenant string, limits Limits, n int, label string) {
	t.Helper()
	for i := 0; i < n; i++ {
		if d, err := l.Allow(ctx, tenant, limits, 0); err != nil || !d.Allowed {
			t.Fatalf("%s request %d: allowed=%v err=%v", label, i, d.Allowed, err)
		}
	}
}

// mustReject sends one request that must be denied: no backend error,
// and no permission.
func mustReject(t *testing.T, ctx context.Context, l Limiter, tenant string, limits Limits, label string) {
	t.Helper()
	if d, err := l.Allow(ctx, tenant, limits, 0); err != nil || d.Allowed {
		t.Fatalf("%s: %v %v", label, d.Allowed, err)
	}
}
