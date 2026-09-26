/**
 * @file insights_test
 * @description The record buffer and writer loop: bounded queueing,
 * drop accounting under capacity pressure and after shutdown, the
 * flush-on-close and flush-on-interval paths, and the CopyFrom row
 * source.
 */
package insights

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

// scriptedSink counts inserted records.
type scriptedSink struct {
	inserted atomic.Int64
}

func (s *scriptedSink) insert(ctx context.Context, batch []Record) {
	s.inserted.Add(int64(len(batch)))
}

// fakeCopier scripts CopyFrom outcomes.
type fakeCopier struct {
	rows int64
	err  error
}

func (f *fakeCopier) CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error) {
	return f.rows, f.err
}

// TestCopyIntoDropsOnFailure: a failed batch increments the drop
// counter by the batch size and never retries.
func TestCopyIntoDropsOnFailure(t *testing.T) {
	t.Parallel()
	s := &PGStore{wake: make(chan struct{}, 1), done: make(chan struct{})}
	batch := []Record{{Status: 200}, {Status: 502}}

	copyInto(context.Background(), &fakeCopier{err: errors.New("copy failed")}, s, batch)
	if got := s.Dropped(); got != 2 {
		t.Fatalf("dropped = %d, want 2", got)
	}

	copyInto(context.Background(), &fakeCopier{rows: 2}, s, batch)
	if got := s.Dropped(); got != 2 {
		t.Fatalf("dropped = %d after a successful batch, want unchanged", got)
	}
}

func TestRecordQueuesAndCountsDrops(t *testing.T) {
	t.Parallel()
	s := &PGStore{wake: make(chan struct{}, 1), done: make(chan struct{})}

	for i := 0; i < batchSize*8; i++ {
		s.Record(Record{Time: time.Now(), TenantID: "t1", Status: 200})
	}
	// The queue is exactly at capacity; every further record drops.
	s.Record(Record{Status: 200})
	if got := s.Dropped(); got != 1 {
		t.Fatalf("dropped = %d, want 1", got)
	}
}

// TestRecordAfterCloseDrops: records arriving after shutdown are
// counted, never buffered into a dead writer.
func TestRecordAfterCloseDrops(t *testing.T) {
	t.Parallel()
	s := &PGStore{wake: make(chan struct{}, 1), done: make(chan struct{})}
	s.closed = true

	s.Record(Record{Status: 200})
	if got := s.Dropped(); got != 1 {
		t.Fatalf("dropped = %d, want 1 after close", got)
	}
}

// TestRecordWakesWriterOnFull: filling the batch signals the writer
// without blocking, and a second signal is absorbed.
func TestRecordWakesWriterOnFull(t *testing.T) {
	t.Parallel()
	s := &PGStore{wake: make(chan struct{}, 1), done: make(chan struct{})}

	for i := 0; i < batchSize; i++ {
		s.Record(Record{Status: 200})
	}
	select {
	case <-s.wake:
	default:
		t.Fatal("a full batch did not wake the writer")
	}
	// The wake channel is drained; more records cannot block.
	for i := 0; i < batchSize*7; i++ {
		s.Record(Record{Status: 200})
	}
	if got := len(s.queue); got != batchSize*8 {
		t.Fatalf("queue = %d, want the bounded %d", got, batchSize*8)
	}
}

// TestWriteLoopFlushesOnClose: records buffered before Close reach
// the (scripted) sink and the loop exits.
func TestWriteLoopFlushesOnClose(t *testing.T) {
	t.Parallel()
	s := &PGStore{wake: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{})}
	sink := &scriptedSink{}
	s.insert = sink.insert
	go s.writeLoop()

	for i := 0; i < batchSize+5; i++ {
		s.Record(Record{Status: 200})
	}
	close(s.stop)
	<-s.done

	if got := sink.inserted.Load(); got != int64(batchSize+5) {
		t.Fatalf("inserted = %d, want %d", got, batchSize+5)
	}
}

// TestWriteLoopFlushesOnInterval: the ticker flushes a partial batch
// without Close.
func TestWriteLoopFlushesOnInterval(t *testing.T) {
	t.Parallel()
	s := &PGStore{wake: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{})}
	sink := &scriptedSink{}
	s.insert = sink.insert
	go s.writeLoop()
	defer func() {
		close(s.stop)
		<-s.done
	}()

	s.Record(Record{Status: 200})
	deadline := time.Now().Add(5 * time.Second)
	for sink.inserted.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if sink.inserted.Load() != 1 {
		t.Fatalf("inserted = %d, want the partial batch flushed", sink.inserted.Load())
	}
}

func TestRecordSourceFeedsRowsInOrder(t *testing.T) {
	t.Parallel()
	when := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	src := copySource([]Record{
		{Time: when, TenantID: "t1", KeyID: "k1", RequestID: "r1", Model: "m", Upstream: "u",
			Path: "/v1/chat/completions", Status: 200, DurationMS: 42, Tokens: 7, CacheHit: true, Streamed: true},
		{Time: when, Status: 502, ErrorCode: "upstream_unreachable"},
	})

	if !src.Next() {
		t.Fatal("source ended early")
	}
	row, err := src.Values()
	if err != nil {
		t.Fatalf("values: %v", err)
	}
	if row[1] != "t1" || row[7] != 200 || row[8] != int64(42) || row[10] != true || row[12] != "" {
		t.Fatalf("row = %v", row)
	}
	if !src.Next() {
		t.Fatal("source ended before the second row")
	}
	row, _ = src.Values()
	if row[7] != 502 || row[12] != "upstream_unreachable" {
		t.Fatalf("failure row = %v", row)
	}
	if src.Next() {
		t.Fatal("source continues past its batch")
	}
	if src.Err() != nil {
		t.Fatalf("err = %v", src.Err())
	}
}

// TestNewPGStoreRejectsInvalidDSN: a broken DSN fails at construction,
// not at the first record.
func TestNewPGStoreRejectsInvalidDSN(t *testing.T) {
	t.Parallel()
	if _, err := NewPGStore(context.Background(), "not a valid dsn"); err == nil {
		t.Fatal("NewPGStore accepted an invalid dsn")
	}
}

// TestRecordConcurrentWithClose: records racing the shutdown never send
// on a closed channel and never land in a dead queue — the wake channel
// is never closed, only the loop's stop channel is. The protocol is
// Close's own minus the pool close (a bare store has none).
func TestRecordConcurrentWithClose(t *testing.T) {
	t.Parallel()
	s := &PGStore{wake: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{})}
	sink := &scriptedSink{}
	s.insert = sink.insert
	go s.writeLoop()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			s.Record(Record{Status: 200})
		}
	}()
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	close(s.stop)
	<-s.done
	wg.Wait()
}
