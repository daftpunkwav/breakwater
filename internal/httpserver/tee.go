/**
 * @file tee
 * @description Response tee: captures what a handler writes while
 * passing every byte through unchanged.
 *
 * Responsibilities:
 * - Mirror status, body bytes and flush activity out of the write path
 * - Preserve streaming semantics: the tee is always an http.Flusher so
 *   SSE flush assertions survive handler chains (net/http memo)
 *
 * Two capture modes:
 * - buffering: accumulates the body up to a byte cap; beyond the cap the
 *   bytes still reach the client but are no longer retained
 * - counting: records byte count only, never retains content — the
 *   streaming default, so proxied bodies do not pile up in memory
 *
 * The tee never reorders or withholds writes; it observes them.
 */
package httpserver

import (
	"bytes"
	"net/http"
)

// TeeResponseWriter wraps an http.ResponseWriter, forwarding every call
// to the underlying writer while recording what passed through.
type TeeResponseWriter struct {
	w       http.ResponseWriter
	flusher http.Flusher
	// captureLimit bounds retained body bytes; 0 selects counting mode.
	captureLimit int
	// detached marks a tee whose downstream write failures must not
	// abort the handler chain (see NewDetachedBufferingTee).
	detached bool

	status      int
	wroteHeader bool
	body        bytes.Buffer
	truncated   bool
	bytes       int64
	flushed     bool
}

// NewDetachedBufferingTee builds a tee for work whose result is shared
// with other requests. It forwards to w and retains the body, but a
// failed write no longer aborts the handler chain: a client that
// disappeared mid-flight is nobody's business but its own, and letting
// its write error propagate would fail every request waiting on the same
// shared fetch. Pair it with a context detached from that client's
// cancellation.
func NewDetachedBufferingTee(w http.ResponseWriter, captureLimit int) *TeeResponseWriter {
	return &TeeResponseWriter{w: w, captureLimit: captureLimit, flusher: flusherOf(w), detached: true}
}

// flusherOf is the flusher lookup newTee shares.
func flusherOf(w http.ResponseWriter) http.Flusher {
	if f, ok := w.(http.Flusher); ok {
		return f
	}
	return nil
}

// NewBufferingTee builds a tee that retains the body up to captureLimit
// bytes; writes past the limit pass through but are dropped from the
// retained copy. The limit must be positive.
func NewBufferingTee(w http.ResponseWriter, captureLimit int) *TeeResponseWriter {
	if captureLimit < 0 {
		captureLimit = 0
	}
	return newTee(w, captureLimit)
}

// NewCountingTee builds a tee that only records byte counts; body bytes
// are never retained. Use it for streaming responses.
func NewCountingTee(w http.ResponseWriter) *TeeResponseWriter {
	return newTee(w, 0)
}

func newTee(w http.ResponseWriter, captureLimit int) *TeeResponseWriter {
	t := &TeeResponseWriter{w: w, captureLimit: captureLimit, flusher: flusherOf(w)}
	return t
}

// Header exposes the underlying header map.
func (t *TeeResponseWriter) Header() http.Header { return t.w.Header() }

// WriteHeader records the first status code and forwards the call; later
// calls are absorbed (net/http logs superfluous WriteHeader calls).
func (t *TeeResponseWriter) WriteHeader(status int) {
	if t.wroteHeader {
		return
	}
	t.wroteHeader = true
	t.status = status
	t.w.WriteHeader(status)
}

// Write forwards the bytes and mirrors them into the capture buffer.
func (t *TeeResponseWriter) Write(p []byte) (int, error) {
	if !t.wroteHeader {
		t.WriteHeader(http.StatusOK)
	}
	n, err := t.w.Write(p)
	t.bytes += int64(n)
	if t.detached && err != nil {
		// The client this tee happens to point at is gone. The capture
		// still completes, so the shared work this tee backs survives
		// and the requests waiting on it are served.
		err = nil
	}
	if t.captureLimit > 0 {
		if room := t.captureLimit - t.body.Len(); room > 0 {
			kept := min(room, len(p))
			t.body.Write(p[:kept])
			t.truncated = kept < len(p)
		} else {
			t.truncated = true
		}
	}
	return n, err
}

// Flush forwards a flush to the underlying writer when it supports one.
// The method exists unconditionally so handler chains can assert
// http.Flusher on the tee without leaking whether deeper layers do.
func (t *TeeResponseWriter) Flush() {
	t.flushed = true
	if t.flusher != nil {
		t.flusher.Flush()
	}
}

// Status reports the status code sent to the client, or 0 when the
// handler has not written a header yet.
func (t *TeeResponseWriter) Status() int { return t.status }

// Body returns the retained body bytes. In counting mode it is empty;
// past the capture limit it is a truncated prefix.
func (t *TeeResponseWriter) Body() []byte { return t.body.Bytes() }

// Truncated reports whether body bytes passed through unretained because
// the capture limit was exceeded.
func (t *TeeResponseWriter) Truncated() bool { return t.truncated }

// BytesWritten reports the total body byte count that reached the client.
func (t *TeeResponseWriter) BytesWritten() int64 { return t.bytes }

// Flushed reports whether the handler flushed at least once — the signal
// that a streaming response started reaching the client.
func (t *TeeResponseWriter) Flushed() bool { return t.flushed }
