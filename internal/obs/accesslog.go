/**
 * @file accesslog
 * @description Access log contracts: the entry schema and the sink port.
 *
 * Contract points (invariant I7):
 * - Record never blocks the request path; capacity pressure ends in
 *   explicit drops that are counted, never in backpressure on handlers
 * - Flush exists for graceful shutdown only (invariant I8: the queue is
 *   drained before exit); it is never called on the request path
 *
 * Implementations must be safe for concurrent use.
 */
package obs

import (
	"context"
	"time"
)

// Entry is one access record for a finished request. Fields are append
// only across releases; consumers must tolerate additions.
type Entry struct {
	Time     time.Time
	TenantID string
	// KeyID is the API key identity the request resolved through
	// (PostgreSQL deployments); the static identity mode leaves it empty.
	KeyID     string
	RequestID string
	// Model and Upstream carry the dimensions the evidence documents are
	// derived from: per-model traffic, per-upstream errors, cache
	// economics.
	Model    string
	Upstream string
	Method   string
	Path     string
	Status   int
	Duration time.Duration
	// CacheHit reports whether the response came from the cache; the
	// "exactly one upstream fetch per cold key" evidence (invariant I2)
	// is audited from this field.
	CacheHit bool
	// Tokens is the settled token usage the request consumed; zero for
	// rejections and failed forwards.
	Tokens int64
	// Streamed reports a streaming passthrough that started.
	Streamed bool
	// ErrorCode carries the failure classification of a rejected or
	// failed request: the governance rejection code (invalid_api_key,
	// rate_limit_exceeded, insufficient_quota,
	// concurrency_limit_exceeded, ...) or the relay's gateway/abort
	// code. Empty on success.
	ErrorCode string
}

// StatusClientClosedRequest marks a request that ended without an HTTP
// response: the client disconnected (or the handler died) before a
// status was written. The aggregation never counts it as a failure.
const StatusClientClosedRequest = 499

// Sink consumes access entries asynchronously.
type Sink interface {
	// Record accepts one access entry without blocking the request path.
	Record(entry Entry)
	// Flush drains pending entries to underlying storage, returning when
	// done or ctx is done. Shutdown path only.
	Flush(ctx context.Context) error
}
