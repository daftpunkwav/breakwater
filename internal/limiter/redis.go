/**
 * @file redis
 * @description The Redis limiter backend: the token bucket semantics
 * executed atomically by Lua.
 *
 * Responsibilities:
 * - Run the refill-check-deduct sequence in one atomic script so
 *   concurrent tenants cannot overdraw a bucket
 * - Nothing else: keyspace layout and script text are implementation
 *   details; the failure mode (backend unreachable) is returned to the
 *   caller, whose pipeline layer owns the degradation policy
 */
package limiter

import (
	"context"
	_ "embed"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// allowScriptSrc is the atomic refill-check-deduct token bucket script
// behind Allow.
//
//go:embed tokenbucket.lua
var allowScriptSrc string

// refundScriptSrc refunds tokens into the TPM bucket.
//
//go:embed refund.lua
var refundScriptSrc string

// defaultKeyPrefix namespaces every limiter key when the deployment has
// no namespace of its own. Safe only when this gateway owns its Redis
// instance: buckets are per-tenant state that two environments sharing a
// Redis would throttle against each other.
const defaultKeyPrefix = "bw:limiter:"

// Redis is the Redis-backed Limiter. It is safe for concurrent use.
type Redis struct {
	rdb       *redis.Client
	allow     *redis.Script
	refund    *redis.Script
	keyPrefix string
}

// NewRedis builds the backend over a ready client. namespace scopes this
// deployment's buckets inside a shared Redis instance; empty means this
// gateway owns the instance outright.
func NewRedis(client *redis.Client, namespace string) *Redis {
	return &Redis{
		rdb:       client,
		allow:     redis.NewScript(allowScriptSrc),
		refund:    redis.NewScript(refundScriptSrc),
		keyPrefix: namespacedKeyPrefix(namespace),
	}
}

// namespacedKeyPrefix places a deployment namespace in front of the
// package's own key base, so two environments sharing one Redis
// instance never throttle against each other's buckets.
func namespacedKeyPrefix(namespace string) string {
	if namespace == "" {
		return defaultKeyPrefix
	}
	return namespace + ":" + defaultKeyPrefix
}

// Ping reports backend health for readiness probes.
func (r *Redis) Ping(ctx context.Context) error {
	return r.rdb.Ping(ctx).Err()
}

// Allow implements Limiter.
func (r *Redis) Allow(ctx context.Context, tenantID string, limits Limits, tokens int64) (Decision, error) {
	res, err := r.allow.Run(ctx, r.rdb,
		[]string{r.rpmKey(tenantID), r.tpmKey(tenantID)},
		limits.RPM, limits.TPM, tokens,
	).Slice()
	if err != nil {
		return Decision{}, fmt.Errorf("limiter: allow script: %w", err)
	}
	allowed, _ := res[0].(int64)
	retryMs, _ := res[1].(int64)
	return Decision{
		Allowed:    allowed == 1,
		RetryAfter: time.Duration(retryMs) * time.Millisecond,
	}, nil
}

// Refund implements Limiter.
func (r *Redis) Refund(ctx context.Context, tenantID string, limits Limits, tokens int64) error {
	if tokens <= 0 || limits.TPM <= 0 {
		return nil
	}
	err := r.refund.Run(ctx, r.rdb,
		[]string{r.tpmKey(tenantID)},
		limits.TPM, tokens,
	).Err()
	if err != nil {
		return fmt.Errorf("limiter: refund script: %w", err)
	}
	return nil
}

func (r *Redis) rpmKey(tenantID string) string { return r.keyPrefix + tenantID + ":rpm" }
func (r *Redis) tpmKey(tenantID string) string { return r.keyPrefix + tenantID + ":tpm" }
