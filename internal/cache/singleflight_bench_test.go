/**
 * @file singleflight_bench_test
 * @description Cost of joining an in-flight fetch: the waiter path is
 * what a cache stampede multiplies, so its per-waiter cost is tracked
 * here — one held flight, a fixed fan of joining waiters per iteration.
 */
package cache

import (
	"context"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func BenchmarkFlightStampedeJoin(b *testing.B) {
	const waiters = 16
	g := NewFlight()
	ctx := context.Background()
	var missed atomic.Int64 // a join that became an owner instead

	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := "k" + strconv.Itoa(i)
		hold := make(chan struct{})
		ownerDone := make(chan struct{})
		go func() {
			defer close(ownerDone)
			_, _, _ = g.Do(ctx, key, func(context.Context) (Entry, error) {
				<-hold
				return entryFor(200), nil
			})
		}()
		// Wait until the flight is registered so the fan joins a held
		// fetch, not an empty key.
		for {
			g.mu.Lock()
			_, registered := g.calls[key]
			g.mu.Unlock()
			if registered {
				break
			}
			runtime.Gosched()
		}

		var wg sync.WaitGroup
		for j := 0; j < waiters; j++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				_, _, owner := g.Do(ctx, key, func(context.Context) (Entry, error) {
					return entryFor(200), nil
				})
				if owner {
					missed.Add(1)
				}
			}()
		}
		// Let the fan pile on before releasing the fetch; the misses
		// guard below catches iterations measured wrong under heavy
		// machine load, where late goroutines would join as owners.
		time.Sleep(200 * time.Microsecond)
		close(hold)
		wg.Wait()
		<-ownerDone
	}
	if n := missed.Load(); n > int64(b.N)*waiters/2 {
		b.Errorf("waiters joined as owners %d times of %d joins; benchmark measures the wrong path", n, int64(b.N)*waiters)
	}
}
