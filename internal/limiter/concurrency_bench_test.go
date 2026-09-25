/**
 * @file concurrency_bench_test
 * @description The concurrency gate's locking cost profile: the gate is
 * one mutex over one map, shared by every tenant. These benchmarks pin
 * that ceiling — the serializing lock is the deliberate, honest design
 * (a map index plus an increment per request) unless a measured
 * regression says otherwise.
 */
package limiter

import (
	"strconv"
	"sync/atomic"
	"testing"
)

// BenchmarkConcurrencyAcquireRelease measures the uncontended floor:
// two lock round trips per request, one on acquire and one on release.
func BenchmarkConcurrencyAcquireRelease(b *testing.B) {
	g := NewConcurrency()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		release, ok := g.Acquire("bench", 1<<40)
		if !ok {
			b.Fatal("acquire failed")
		}
		release()
	}
}

// BenchmarkConcurrencyContended hammers the gate from many goroutines
// across many tenants — the request-rate ceiling of the shared lock.
// Run with -cpu 1,4,16 to see the contention curve.
func BenchmarkConcurrencyContended(b *testing.B) {
	g := NewConcurrency()
	const tenants = 64
	var seq atomic.Int64
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		// One stable tenant per worker goroutine, mimicking the
		// production mix of many identities under one gate.
		tenant := "tenant-" + strconv.FormatInt(seq.Add(1)%tenants, 10)
		for pb.Next() {
			release, ok := g.Acquire(tenant, 1<<40)
			if !ok {
				b.Fatal("acquire failed")
			}
			release()
		}
	})
}
