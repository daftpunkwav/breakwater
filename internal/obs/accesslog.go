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
}

// Sink consumes access entries asynchronously.
type Sink interface {
	// Record accepts one access entry without blocking the request path.
	Record(entry Entry)
	// Flush drains pending entries to underlying storage, returning when
	// done or ctx is done. Shutdown path only.
	Flush(ctx context.Context) error
}
