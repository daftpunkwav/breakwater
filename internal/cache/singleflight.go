/**
 * @file singleflight
 * @description The hand-written in-flight deduplication: concurrent
 * callers sharing one cache key share one fetch.
 *
 * Responsibilities:
 * - Guarantee that a cold key's concurrent non-streaming requests
 *   produce exactly one upstream fetch (invariant I2)
 * - Bound every waiter by its own request context: waiters never
 *   inherit the holder's remaining deadline, and a waiter whose client
 *   leaves stops waiting (net/http memo §9 note)
 * - Share the holder's result — including its errors — with the
 *   waiters; retrying is the retry layer's business, never implicit
 * - Nothing else: eligibility and storage live with their owners
 *
 * Discipline note: x/sync/singleflight is rejected by lint rule; this
 * implementation is the project's own statement of the mechanism.
 */
package cache

import (
	"context"
	"fmt"
	"sync"
)

// call is one in-flight fetch: the holder runs fn, waiters block on
// the WaitGroup and read the result after Done.
type call struct {
	wg    sync.WaitGroup
	value Entry
	err   error
}

// Flight deduplicates concurrent fetches per key. It is safe for
// concurrent use.
type Flight struct {
	mu    sync.Mutex
	calls map[string]*call
}

// NewFlight builds the flight group.
func NewFlight() *Flight {
	return &Flight{calls: make(map[string]*call)}
}

// Do runs fn for the key, deduplicating concurrent callers. owner
// reports whether this invocation actually executed fn (true) or
// attached to an existing flight (false).
func (g *Flight) Do(ctx context.Context, key string, fn func(context.Context) (Entry, error)) (Entry, error, bool) {
	g.mu.Lock()
	if existing, ok := g.calls[key]; ok {
		g.mu.Unlock()
		entry, err := existing.wait(ctx)
		return entry, err, false
	}
	c := &call{}
	c.wg.Add(1)
	g.calls[key] = c
	g.mu.Unlock()

	// Holder path. The result is published before Done so waiting
	// readers observe it through the WaitGroup's happens-before.
	defer func() {
		if r := recover(); r != nil {
			// A panicking fetch must not wedge the flight: publish the
			// failure, release the waiters, then re-raise so the caller's
			// recovery (net/http's per-connection handler) sees it.
			c.err = fmt.Errorf("cache: fetch panicked: %v", r)
			g.mu.Lock()
			delete(g.calls, key)
			g.mu.Unlock()
			c.wg.Done()
			panic(r)
		}
	}()
	value, err := fn(ctx)
	c.value, c.err = value, err

	g.mu.Lock()
	delete(g.calls, key)
	g.mu.Unlock()
	c.wg.Done()

	return c.value, c.err, true
}

// wait blocks for the holder's result, bounded by the waiter's own
// context: a client gone mid-wait stops waiting.
func (c *call) wait(ctx context.Context) (Entry, error) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		c.wg.Wait()
	}()
	select {
	case <-done:
		return c.value, c.err
	case <-ctx.Done():
		return Entry{}, ctx.Err()
	}
}
