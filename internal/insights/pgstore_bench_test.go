/**
 * @file pgstore_bench_test
 * @description Cost of one enqueue-flush cycle on the batch queue: the
 * writer flushes every interval or full batch, so the queue's
 * reallocation behavior per cycle is what sustained traffic pays.
 */
package insights

import (
	"context"
	"testing"
	"time"
)

// BenchmarkRecordBatchCycle measures one full cycle: enqueue batchSize
// records, then flush them to a no-op sink. The allocation profile
// exposes how the queue's backing array is treated across cycles.
func BenchmarkRecordBatchCycle(b *testing.B) {
	s := &PGStore{
		insert: func(context.Context, []Record) {},
		wake:   make(chan struct{}, 1),
	}
	r := Record{
		Time:       time.Now(),
		TenantID:   "tenant",
		KeyID:      "key",
		RequestID:  "req-0123456789abcdef",
		Model:      "mock-model",
		Upstream:   "mock-upstream",
		Path:       "/v1/chat/completions",
		Status:     200,
		DurationMS: 12,
		Tokens:     100,
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for j := 0; j < batchSize; j++ {
			s.Record(r)
		}
		s.flush()
	}
}
