/**
 * @file logger_test
 * @description Access log tests: async drain, drop-oldest accounting
 * under pressure (invariant I7) and the shutdown flush (I8).
 */
package obs

import (
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
		t.Fatalf("buffered = %d, want 0 after close (I8)", got)
	}
	if !strings.Contains(out.String(), `"Status":4`) && !strings.Contains(out.String(), `"status":4`) {
		t.Fatalf("last entry missing from %q", out.String())
	}
}
