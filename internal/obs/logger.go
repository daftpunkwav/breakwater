/**
 * @file logger
 * @description The asynchronous access log: a bounded queue drained to
 * JSON lines by one background goroutine.
 *
 * Responsibilities:
 * - Accept entries without ever blocking the request path (invariant
 *   I7): capacity pressure drops the oldest entries and counts them —
 *   the drop counter rising is normal, dropping silently is not
 * - Persist entries as JSON lines to the configured sink
 * - Flush on shutdown only (invariant I8): the queue is drained before
 *   the process exits, bounded by the caller's deadline
 * - Nothing else: no formatting policy beyond JSON, no destinations
 *
 * The queue is a mutex-guarded ring, not a channel: drop-oldest needs
 * eviction, which channel semantics cannot express.
 */
package obs

import (
	"context"
	"encoding/json"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

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

	out    io.Writer
	signal chan struct{}
	done   chan struct{}
	wg     sync.WaitGroup
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

// Close drains the queue and stops the drain goroutine.
func (l *Logger) Close(ctx context.Context) error {
	close(l.done)
	l.wg.Wait()
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

// drain writes every queued entry as one JSON line each.
func (l *Logger) drain() {
	l.draining.Store(true)
	defer l.draining.Store(false)
	for {
		l.mu.Lock()
		if l.count == 0 {
			l.mu.Unlock()
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
		if _, err := l.out.Write(raw); err != nil {
			l.drops.Add(1)
			return
		}
		l.written.Add(1)
	}
}
