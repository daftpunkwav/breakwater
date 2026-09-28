/**
 * @file logger_test
 * @description Access log tests: async drain, drop-oldest accounting
 * under pressure and the shutdown flush.
 */
package obs

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestLoggerDrainsEntries(t *testing.T) {
	t.Parallel()
	var out bytes.Buffer
	l := NewLogger(&out, 64)

	l.Record(Entry{TenantID: "t1", Path: "/v1/chat/completions", Status: 200})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := l.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	line := strings.TrimSpace(out.String())
	if line == "" {
		t.Fatal("nothing written")
	}
	var entry Entry
	if err := json.Unmarshal([]byte(line), &entry); err != nil {
		t.Fatalf("not valid JSONL: %v", err)
	}
	if entry.TenantID != "t1" || entry.Status != 200 {
		t.Fatalf("entry = %+v", entry)
	}
}

// TestLoggerWritesAttemptTrail: the attempt trail survives the JSONL
// round trip as the failover walk the request took.
func TestLoggerWritesAttemptTrail(t *testing.T) {
	t.Parallel()
	var out bytes.Buffer
	l := NewLogger(&out, 8)

	l.Record(Entry{Status: 200, Attempts: []AttemptTrace{
		{Upstream: "up-1", Credential: 0, Status: 429},
		{Upstream: "up-2", Credential: 1, Status: 200},
	}})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := l.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	var entry Entry
	if err := json.Unmarshal([]byte(strings.TrimSpace(out.String())), &entry); err != nil {
		t.Fatalf("not valid JSONL: %v", err)
	}
	if len(entry.Attempts) != 2 ||
		entry.Attempts[0].Upstream != "up-1" || entry.Attempts[0].Credential != 0 || entry.Attempts[0].Status != 429 ||
		entry.Attempts[1].Upstream != "up-2" || entry.Attempts[1].Credential != 1 || entry.Attempts[1].Status != 200 {
		t.Fatalf("attempts = %+v, want the two-step walk", entry.Attempts)
	}
}

func TestLoggerDropsOldestUnderPressure(t *testing.T) {
	t.Parallel()
	var out bytes.Buffer
	l := NewLogger(&out, 4)

	for i := 0; i < 10; i++ {
		l.Record(Entry{Status: i})
	}
	if got := l.Dropped(); got != 6 {
		t.Fatalf("dropped = %d, want 6 (oldest evicted)", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := l.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 4 {
		t.Fatalf("written = %d lines, want 4 (the newest)", len(lines))
	}
	// The survivors are the four newest entries, in order.
	for i, line := range lines {
		var entry Entry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			t.Fatalf("line %d: %v", i, err)
		}
		if want := 6 + i; entry.Status != want {
			t.Fatalf("line %d status = %d, want %d", i, entry.Status, want)
		}
	}
}

func TestLoggerConcurrentRecorders(t *testing.T) {
	t.Parallel()
	var out bytes.Buffer
	l := NewLogger(&out, 1024)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				l.Record(Entry{Status: 200})
			}
		}()
	}
	wg.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := l.Flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if got := l.Written(); got != 400 {
		t.Fatalf("written = %d, want 400", got)
	}
}

func TestLoggerCloseDrains(t *testing.T) {
	t.Parallel()
	var out bytes.Buffer
	l := NewLogger(&out, 16)
	for i := 0; i < 5; i++ {
		l.Record(Entry{Status: i})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := l.Close(ctx); err != nil {
		t.Fatalf("close: %v", err)
	}
	if got := l.Buffered(); got != 0 {
		t.Fatalf("buffered = %d, want 0 after close", got)
	}
	if !strings.Contains(out.String(), `"Status":4`) && !strings.Contains(out.String(), `"status":4`) {
		t.Fatalf("last entry missing from %q", out.String())
	}
}

// countingWriter counts sink writes; for the file sink each one is a
// syscall.
type countingWriter struct{ writes int }

func (w *countingWriter) Write(p []byte) (int, error) {
	w.writes++
	return len(p), nil
}

// TestLoggerBatchesSinkWrites pins the drain's batching: entries queued
// between two drain passes leave in whole-buffer sink writes, not one
// write per entry, with the written counter settled by the pass.
func TestLoggerBatchesSinkWrites(t *testing.T) {
	t.Parallel()
	out := &countingWriter{}
	// The drain loop is the drain method's only production caller; the
	// benchmark and this test drive it directly for determinism.
	l := &Logger{ring: make([]Entry, 64), out: out, buf: bufio.NewWriterSize(out, logWriteBufferSize)}

	for i := 0; i < 10; i++ {
		l.Record(Entry{Status: i})
	}
	l.drain()

	if out.writes != 1 {
		t.Fatalf("sink writes = %d, want 1 for one batched pass", out.writes)
	}
	if got := l.Written(); got != 10 {
		t.Fatalf("written = %d, want 10 after the pass", got)
	}
	if got := l.Buffered(); got != 0 {
		t.Fatalf("buffered = %d, want 0 after the pass", got)
	}
}
