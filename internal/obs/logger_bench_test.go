/**
 * @file logger_bench_test
 * @description Cost of draining access log entries to their sink: the
 * per-entry sink writes are what sustained traffic costs in syscalls.
 * The batch shape is the production one — records pile up between
 * wakeups and one drain pass moves them all — so the writes-per-entry
 * ratio is tracked beside time and allocations.
 */
package obs

import (
	"bufio"
	"testing"
	"time"
)

// countingSink counts the writes it receives; each write is one sink
// call (one syscall for the file sink).
type countingSink struct{ writes int }

func (s *countingSink) Write(p []byte) (int, error) {
	s.writes++
	return len(p), nil
}

// benchLogger builds a Logger without its background drain loop: drain
// is the loop goroutine's method alone in production, and the benchmark
// drives it from its own goroutine to measure the same single-drainer
// path.
func benchLogger(out *countingSink) *Logger {
	return &Logger{
		ring: make([]Entry, 4096),
		out:  out,
		buf:  bufio.NewWriterSize(out, logWriteBufferSize),
	}
}

// drainBatch enqueues n entries and moves them to the sink in one
// drain pass, the way the drain goroutine serves a woken queue.
func drainBatch(l *Logger, entry Entry, n int) {
	for i := 0; i < n; i++ {
		l.Record(entry)
	}
	l.drain()
}

// BenchmarkLoggerDrainBatch measures one drain pass over 200 entries.
func BenchmarkLoggerDrainBatch(b *testing.B) {
	const perBatch = 200
	out := &countingSink{}
	l := benchLogger(out)
	entry := Entry{
		Time:      time.Now(),
		TenantID:  "tenant",
		RequestID: "req-0123456789abcdef",
		Model:     "mock-model",
		Upstream:  "mock-upstream",
		Method:    "POST",
		Path:      "/v1/chat/completions",
		Status:    200,
		Duration:  123 * time.Millisecond,
		Tokens:    4321,
	}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		drainBatch(l, entry, perBatch)
	}
	b.ReportMetric(float64(out.writes)/float64(b.N*perBatch), "sink-writes/entry")
}
