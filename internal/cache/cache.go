/**
 * @file cache
 * @description Exact-match response cache contracts.
 *
 * Responsibilities:
 * - Define the stored entry and the storage port
 * - Nothing else: singleflight and the eligibility policy
 *   (deterministic parameter combinations only) live with their owners —
 *   as coordination around the port, not inside it
 *
 * Contract points:
 * - Ownership: Get must return an Entry whose Header is a private copy;
 *   callers may read but never mutate it. Cached entries are replayed
 *   to concurrent clients, so shared map state would be a data race
 * - Deadline isolation: a singleflight implementation must bound each
 *   waiter by its own request context; waiters never inherit the
 *   holder's remaining deadline
 * - TTL: Set receives the base TTL; the implementation applies jitter
 *   on top so expirations do not align into a storm
 * - Degradation: the cache is an optimization, never a correctness
 *   dependency; a failing backend degrades to direct upstream forwarding
 */
package cache

import (
	"context"
	"errors"
	"net/http"
	"time"
)

// ErrMiss reports a cache miss; callers proceed with the normal path.
var ErrMiss = errors.New("cache: miss")

// Entry is one cached upstream response.
type Entry struct {
	Status int
	Header http.Header
	Body   []byte
}

// Cache stores exact-match request responses keyed by content hash.
// Implementations must be safe for concurrent use.
type Cache interface {
	// Get returns ErrMiss when the key has no live entry; otherwise the
	// entry is a private copy per the ownership rule in the file header.
	Get(ctx context.Context, key string) (Entry, error)
	// Set stores an entry under key for the given base TTL; expiry
	// jitter is added by the implementation. Concurrent writers are
	// allowed; the last write wins.
	Set(ctx context.Context, key string, entry Entry, baseTTL time.Duration) error
}
