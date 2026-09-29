/**
 * @file logger
 * @description The asynchronous access log: a bounded queue drained to
 * JSON lines by one background goroutine, the lines batched into
 * whole-buffer sink writes per drain pass.
 *
 * Responsibilities:
 * - Accept entries without ever blocking the request path: capacity
 *   pressure drops the oldest entries and counts them —
 *   the drop counter rising is normal, dropping silently is not
 * - Persist entries as JSON lines to the configured sink
 * - Flush on shutdown only: the queue is drained before
 *   the process exits, bounded by the caller's deadline
 * - Nothing else: no formatting policy beyond JSON, no destinations
 *
 * The queue is a mutex-guarded ring, not a channel: drop-oldest needs
 * eviction, which channel semantics cannot express.
 */
package obs

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// logWriteBufferSize bounds the drain's write batching: JSON lines
// accumulate in it so one drain pass costs sink writes in whole buffer
// sizes instead of one write per entry.
const logWriteBufferSize = 64 << 10

// Logger is the asynchronous access log sink.
type Logger struct {
	mu      sync.Mutex
	ring    []Entry
	head    int
	count   int
	drops   atomic.Int64
	written atomic.Int64
	// draining reports whether the drain goroutine is mid-write; Flush
	// waits for it to fall, not merely for the queue to empty — a
	// failed write lands in drops after the queue already reads empty.
	draining atomic.Bool

	out io.Writer
	// buf batches JSON lines between sink writes. Owned by the drain
	// goroutine alone; reset after a sink failure so a recovered
	// destination serves the next drain.
	// pending counts entries handed to buf since the last settle, so a
	// failed flush charges exactly that batch to the drop counter.
	buf     *bufio.Writer
	pending int
	signal  chan struct{}
	done    chan struct{}
	// closeOnce guards the done channel's close: Close is idempotent,
	// so a shutdown path that fires twice (an explicit Close after the
	// deferred one) cannot panic on the second close.
	closeOnce sync.Once
	wg        sync.WaitGroup
}

// NewLogger starts the drain goroutine writing JSON lines to out. The
// queue holds capacity entries; overflow drops the oldest and counts.
// Call Close before process exit to drain and stop.
func NewLogger(out io.Writer, capacity int) *Logger {
	if capacity < 1 {
		capacity = 1
	}
	l := &Logger{
		ring:   make([]Entry, capacity),
		out:    out,
		buf:    bufio.NewWriterSize(out, logWriteBufferSize),
		signal: make(chan struct{}, 1),
		done:   make(chan struct{}),
	}
	l.wg.Add(1)
	go l.loop()
	return l
}

// Record implements Sink: enqueue without blocking, drop the oldest on
// pressure.
func (l *Logger) Record(entry Entry) {
	l.mu.Lock()
	if l.count == len(l.ring) {
		// Full: evict the oldest.
		l.head = (l.head + 1) % len(l.ring)
		l.count--
		l.drops.Add(1)
	}
	l.ring[(l.head+l.count)%len(l.ring)] = entry
	l.count++
	l.mu.Unlock()

	select {
	case l.signal <- struct{}{}:
	default:
	}
}

// Dropped reports how many entries were dropped for capacity.
func (l *Logger) Dropped() int64 { return l.drops.Load() }

// Written reports how many entries reached the sink.
func (l *Logger) Written() int64 { return l.written.Load() }

// Buffered reports the entries still queued.
func (l *Logger) Buffered() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.count
}

// Flush implements Sink: waits until every dequeued entry has finished
// writing (or failing into the drop counter) or ctx ends. Shutdown
// path only.
func (l *Logger) Flush(ctx context.Context) error {
	ticker := time.NewTicker(2 * time.Millisecond)
	defer ticker.Stop()
	for {
		l.mu.Lock()
		pending := l.count
		l.mu.Unlock()
		if pending == 0 && !l.draining.Load() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// Close drains the queue and stops the drain goroutine. The wait for
// the drain goroutine is bounded by ctx, like the flush that follows:
// a sink that never accepts the write must hold neither the drain nor
// the process's shutdown past the caller's deadline.
func (l *Logger) Close(ctx context.Context) error {
	l.closeOnce.Do(func() { close(l.done) })
	waited := make(chan struct{})
	go func() {
		l.wg.Wait()
		close(waited)
	}()
	select {
	case <-waited:
	case <-ctx.Done():
	}
	return l.Flush(ctx)
}

// loop drains the ring into the sink.
func (l *Logger) loop() {
	defer l.wg.Done()
	for {
		select {
		case <-l.done:
			l.drain()
			return
		case <-l.signal:
			l.drain()
		}
	}
}

// drain writes every queued entry as one JSON line each, batching the
// lines into whole-buffer sink writes per pass. It never exits with
// unflushed lines, so an empty queue plus an idle drain means every
// dequeued entry reached the sink or was counted dropped.
func (l *Logger) drain() {
	l.draining.Store(true)
	defer l.draining.Store(false)
	for {
		l.mu.Lock()
		if l.count == 0 {
			l.mu.Unlock()
			l.flushBuffer()
			return
		}
		entry := l.ring[l.head]
		l.head = (l.head + 1) % len(l.ring)
		l.count--
		l.mu.Unlock()

		raw, err := json.Marshal(entry)
		if err != nil {
			// A malformed entry is a bug, not a loggable event; count it
			// as dropped to keep the accounting honest.
			l.drops.Add(1)
			continue
		}
		raw = append(raw, '\n')
		if _, err := l.buf.Write(raw); err != nil {
			// The sink refused the entry — a full disk or a closed
			// destination. The stuck entry and whatever sat unflushed
			// beside it are charged to drops; the rest of the queue
			// waits for the next drain, whose writer starts clean so a
			// recovered sink serves it.
			l.drops.Add(1)
			l.chargePending(false)
			l.buf.Reset(l.out)
			return
		}
		l.pending++
	}
}

// flushBuffer pushes the batched lines to the sink at the end of a
// drain pass, settling the pending batch as written or dropped by
// whether the write landed.
func (l *Logger) flushBuffer() {
	if l.pending == 0 {
		return
	}
	if err := l.buf.Flush(); err != nil {
		l.chargePending(false)
		l.buf.Reset(l.out)
		return
	}
	l.chargePending(true)
}

// chargePending settles the pending counter: written when the batch
// reached the sink, dropped when the write failed — a drop counted
// generously on a partial failure, never a write claimed that did not
// land.
func (l *Logger) chargePending(written bool) {
	pending := l.pending
	l.pending = 0
	if pending == 0 {
		return
	}
	if written {
		l.written.Add(int64(pending))
	} else {
		l.drops.Add(int64(pending))
	}
}
