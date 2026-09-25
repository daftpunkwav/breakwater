/**
 * @file logger_write_failure_test
 * @description Sink failure accounting: entries whose write fails are
 * counted as dropped — never silently lost — and a bounded shutdown
 * flush returns the context's error instead of blocking forever.
 */
package obs

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"
)

// failingWriter rejects every write, simulating a full disk or a closed
// log destination.
type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("disk full") }

// gatedWriter blocks every write until its gate is opened, letting the
// test hold the drain goroutine at a deterministic point.
type gatedWriter struct {
	gate chan struct{}
}

func (w gatedWriter) Write(p []byte) (int, error) {
	<-w.gate
	return len(p), nil
}

// TestLoggerCountsFailedWritesAsDropped pins the honest accounting: a
// sink that cannot accept an entry raises the drop counter and never
// the written counter.
func TestLoggerCountsFailedWritesAsDropped(t *testing.T) {
	t.Parallel()
	l := NewLogger(failingWriter{}, 8)
	l.Record(Entry{Status: 200})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := l.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if got := l.Dropped(); got != 1 {
		t.Fatalf("dropped = %d, want 1 (the failed write counted)", got)
	}
	if got := l.Written(); got != 0 {
		t.Fatalf("written = %d, want 0", got)
	}
	if got := l.Buffered(); got != 0 {
		t.Fatalf("buffered = %d, want 0: the failed entry is out of the queue", got)
	}
}

// TestLoggerFlushRespectsContextDeadline pins the shutdown contract: a
// flush whose sink stays blocked gives up at the caller's deadline and
// returns the context's error.
func TestLoggerFlushRespectsContextDeadline(t *testing.T) {
	t.Parallel()
	gate := make(chan struct{})
	l := NewLogger(gatedWriter{gate: gate}, 8)
	t.Cleanup(func() { close(gate) })

	l.Record(Entry{Status: 200})
	l.Record(Entry{Status: 200})

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if err := l.Flush(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("flush err = %v, want DeadlineExceeded", err)
	}
}

// TestLoggerClampsCapacityToAtLeastOne pins the capacity floor: a
// non-positive capacity degenerates to a one-slot queue, not a panic.
func TestLoggerClampsCapacityToAtLeastOne(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	l := NewLogger(&buf, 0)
	if len(l.ring) != 1 {
		t.Fatalf("ring capacity = %d, want 1", len(l.ring))
	}

	l.Record(Entry{Status: 200})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := l.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if l.Written() != 1 {
		t.Fatalf("written = %d, want 1", l.Written())
	}
}
