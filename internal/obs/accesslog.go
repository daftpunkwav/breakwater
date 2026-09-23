/**
 * @file accesslog
 * @description Access log contracts: the entry schema and the sink port.
 *
 * Every implementation must honor invariant I7: observation never blocks
 * the request path. Capacity pressure ends in explicit drops that are
 * counted, never in backpressure on handlers.
 */
package obs

import "time"

// Entry is one access record for a finished request. Fields are append
// only across releases; consumers must tolerate additions.
type Entry struct {
	Time     time.Time
	TenantID string
	Method   string
	Path     string
	Status   int
	Duration time.Duration
}

// Sink consumes access entries asynchronously.
// Implementations must be safe for concurrent use and must not block the
// caller indefinitely.
type Sink interface {
	Record(entry Entry)
}
