/**
 * @file cache
 * @description Exact-match response cache contracts.
 *
 * Responsibilities:
 * - Define the stored entry and the storage port
 * - Nothing else: singleflight, TTL jitter and negative caching belong to
 *   the implementation; eligibility policy (deterministic parameter
 *   combinations only) is applied before a response reaches this port
 *
 * The cache is an optimization, never a correctness dependency: a failing
 * backend must degrade to direct upstream forwarding.
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
	// Get returns ErrMiss when the key has no live entry.
	Get(ctx context.Context, key string) (Entry, error)
	// Set stores an entry under key for the given TTL. Concurrent writers
	// are allowed; the last write wins.
	Set(ctx context.Context, key string, entry Entry, ttl time.Duration) error
}
