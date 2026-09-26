/**
 * @file pgstore
 * @description The PostgreSQL-backed insights store: batched async
 * writes and the aggregation queries.
 *
 * Responsibilities:
 * - Persist records in multi-row batches on a background goroutine
 * - Answer the aggregation queries behind GET /admin/insights
 * - Nothing else: the writer never blocks the request path — a full
 *   buffer drops the record and counts the drop, exactly the access
 *   log's contract (invariant I7)
 */
package insights

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// batching tuning: the writer flushes when either bound is hit.
const (
	batchSize     = 200
	flushInterval = 2 * time.Second
)

// PGStore persists insight records into PostgreSQL and answers the
// aggregation queries. It is safe for concurrent use.
type PGStore struct {
	pool *pgxpool.Pool
	// insert is the batch sink; tests script it. The field keeps the
	// production shape (one method value, set at construction).
	insert func(ctx context.Context, batch []Record)

	mu     sync.Mutex
	queue  []Record
	drops  int64
	wake   chan struct{}
	done   chan struct{}
	closed bool
}

// NewPGStore connects and starts the batch writer; Close flushes and
// stops it.
func NewPGStore(ctx context.Context, dsn string) (*PGStore, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("insights: connect: %w", err)
	}
	s := &PGStore{
		pool: pool,
		wake: make(chan struct{}, 1),
		done: make(chan struct{}),
	}
	s.insert = s.copyBatch
	go s.writeLoop()
	return s, nil
}

// Record accepts one finished request without blocking; capacity
// pressure drops the record and counts the drop (invariant I7).
func (s *PGStore) Record(r Record) {
	s.mu.Lock()
	if s.closed || len(s.queue) >= batchSize*8 {
		s.drops++
		s.mu.Unlock()
		return
	}
	s.queue = append(s.queue, r)
	full := len(s.queue) >= batchSize
	s.mu.Unlock()
	if full {
		select {
		case s.wake <- struct{}{}:
		default:
		}
	}
}

// Dropped reports how many records were dropped for capacity or after
// shutdown.
func (s *PGStore) Dropped() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.drops
}

// Close flushes the pending queue and stops the writer.
func (s *PGStore) Close() {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.mu.Unlock()
	close(s.wake)
	<-s.done
	s.pool.Close()
}

// writeLoop batches records on an interval; the loop owns the queue
// swap so inserts never contend with Record beyond the append. Every
// read of the closed flag happens under the mutex — Close sets it and
// then closes the wake channel, so the loop's channel receive is the
// one synchronization point that matters.
func (s *PGStore) writeLoop() {
	defer close(s.done)
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()
	ctx := context.Background()
	for {
		select {
		case <-s.wake:
		case <-ticker.C:
		}
		s.mu.Lock()
		closed := s.closed
		batch := s.queue
		s.queue = nil
		s.mu.Unlock()
		if len(batch) > 0 {
			s.insert(ctx, batch)
		}
		if closed {
			return
		}
	}
}

// copier is the subset of pgxpool the batch insert needs; the concrete
// pool satisfies it, tests script it.
type copier interface {
	CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error)
}

// copyBatch writes one batch; a failed batch is dropped and counted,
// not retried — the assessment record is best-effort by contract and
// the drop counter keeps the loss visible.
func (s *PGStore) copyBatch(ctx context.Context, batch []Record) {
	copyInto(ctx, s.pool, s, batch)
}

// copyInto is copyBatch's body over the copier port, so the
// drop-on-failure contract is testable without a database.
func copyInto(ctx context.Context, c copier, s *PGStore, batch []Record) {
	insCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	_, err := c.CopyFrom(
		insCtx,
		[]string{"request_log"},
		[]string{"time", "tenant_id", "key_id", "request_id", "model", "upstream", "path", "status", "duration_ms", "tokens", "cache_hit", "streamed", "error_code"},
		copySource(batch),
	)
	if err != nil {
		s.mu.Lock()
		s.drops += int64(len(batch))
		s.mu.Unlock()
	}
}

func copySource(batch []Record) *recordSource {
	return &recordSource{batch: batch}
}

// recordSource feeds one batch to pgx's CopyFrom.
type recordSource struct {
	batch []Record
	i     int
}

func (src *recordSource) Next() bool { return src.i < len(src.batch) }

func (src *recordSource) Values() ([]any, error) {
	r := src.batch[src.i]
	src.i++
	return []any{r.Time, r.TenantID, r.KeyID, r.RequestID, r.Model, r.Upstream, r.Path,
		r.Status, r.DurationMS, r.Tokens, r.CacheHit, r.Streamed, r.ErrorCode}, nil
}

func (src *recordSource) Err() error { return nil }
