/**
 * @file tee_test
 * @description Tee mechanics: capture fidelity, mode behavior, single
 * WriteHeader, flush recording.
 */
package httpserver

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestBufferingTeeCapturesAndForwards(t *testing.T) {
	t.Parallel()
	inner := httptest.NewRecorder()
	tee := NewBufferingTee(inner, 64)

	tee.Header().Set("Content-Type", "text/plain")
	tee.WriteHeader(201)
	if _, err := tee.Write([]byte("hello world")); err != nil {
		t.Fatalf("write: %v", err)
	}

	if tee.Status() != 201 || inner.Code != 201 {
		t.Fatalf("status = %d/%d, want 201", tee.Status(), inner.Code)
	}
	if string(tee.Body()) != "hello world" || inner.Body.String() != "hello world" {
		t.Fatalf("capture mismatch: tee=%q inner=%q", tee.Body(), inner.Body.String())
	}
	if tee.Truncated() {
		t.Error("truncated reported within cap")
	}
}

func TestBufferingTeeDropsPastCapButKeepsForwarding(t *testing.T) {
	t.Parallel()
	inner := httptest.NewRecorder()
	tee := NewBufferingTee(inner, 4)

	if _, err := tee.Write([]byte("abcdefgh")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if string(tee.Body()) != "abcd" {
		t.Fatalf("retained = %q, want truncated prefix abcd", tee.Body())
	}
	if !tee.Truncated() {
		t.Error("truncation not reported")
	}
	if inner.Body.String() != "abcdefgh" {
		t.Fatalf("forwarded = %q, want full body", inner.Body.String())
	}
	if got := tee.BytesWritten(); got != 8 {
		t.Fatalf("bytes written = %d, want 8", got)
	}
}

func TestCountingTeeNeverRetains(t *testing.T) {
	t.Parallel()
	inner := httptest.NewRecorder()
	tee := NewCountingTee(inner)

	if _, err := tee.Write([]byte("stream chunk")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if len(tee.Body()) != 0 {
		t.Fatalf("counting tee retained %q", tee.Body())
	}
	if got := tee.BytesWritten(); got != 12 {
		t.Fatalf("bytes written = %d, want 12", got)
	}
}

func TestTeeWriteHeaderOnce(t *testing.T) {
	t.Parallel()
	inner := httptest.NewRecorder()
	tee := NewBufferingTee(inner, 16)

	tee.WriteHeader(200)
	tee.WriteHeader(500) // must be absorbed

	if inner.Code != 200 {
		t.Fatalf("code = %d, want the first status 200", inner.Code)
	}
}

func TestTeeRecordsFlush(t *testing.T) {
	t.Parallel()
	inner := httptest.NewRecorder()
	tee := NewCountingTee(inner)

	if tee.Flushed() {
		t.Fatal("flushed before any flush")
	}
	tee.Flush()
	if !tee.Flushed() {
		t.Fatal("flush not recorded")
	}
}

// TestBufferingTeeClampsNegativeLimit pins the constructor guard: a
// negative capture limit degenerates to counting mode, never a panic.
func TestBufferingTeeClampsNegativeLimit(t *testing.T) {
	t.Parallel()
	inner := httptest.NewRecorder()
	tee := NewBufferingTee(inner, -1)

	if _, err := tee.Write([]byte("kept nowhere")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if len(tee.Body()) != 0 {
		t.Fatalf("negative-limit tee retained %q", tee.Body())
	}
	if got := tee.BytesWritten(); got != 12 {
		t.Fatalf("bytes written = %d, want 12", got)
	}
}

// TestBufferingTeeMarksTruncationOnLaterWrites pins the truncation flag
// for writes that arrive after the capture buffer is already full: the
// early-write prefix stays retained, later writes only pass through.
func TestBufferingTeeMarksTruncationOnLaterWrites(t *testing.T) {
	t.Parallel()
	inner := httptest.NewRecorder()
	tee := NewBufferingTee(inner, 4)

	if _, err := tee.Write([]byte("abcd")); err != nil {
		t.Fatalf("write 1: %v", err)
	}
	if tee.Truncated() {
		t.Fatal("truncation reported while the buffer had room")
	}
	if _, err := tee.Write([]byte("efgh")); err != nil {
		t.Fatalf("write 2: %v", err)
	}

	if string(tee.Body()) != "abcd" {
		t.Fatalf("retained = %q, want the pre-cap prefix", tee.Body())
	}
	if !tee.Truncated() {
		t.Fatal("post-cap write not reported as truncated")
	}
	if inner.Body.String() != "abcdefgh" {
		t.Fatalf("forwarded = %q, want the full body", inner.Body.String())
	}
}

// brokenWriter stands in for a client connection that has gone away
// mid-response.
type brokenWriter struct {
	header http.Header
	code   int
}

func (b *brokenWriter) Header() http.Header       { return b.header }
func (b *brokenWriter) WriteHeader(code int)      { b.code = code }
func (b *brokenWriter) Write([]byte) (int, error) { return 0, errors.New("broken pipe") }
func (b *brokenWriter) Flush()                    {}

// TestDetachedBufferingTeeSurvivesAGoneClient pins the contract of the
// tee that backs a shared fetch: a client disappearing mid-flight is
// nobody else's business, so the capture still completes and the handler
// chain is not aborted — otherwise every request waiting on the same
// shared fetch would fail with it.
func TestDetachedBufferingTeeSurvivesAGoneClient(t *testing.T) {
	t.Parallel()
	tee := NewDetachedBufferingTee(&brokenWriter{header: make(http.Header)}, 64)

	tee.Header().Set("Content-Type", "text/plain")
	tee.WriteHeader(200)
	if _, err := tee.Write([]byte("shared payload")); err != nil {
		t.Fatalf("write surfaced the client's disconnect: %v", err)
	}

	if got := string(tee.Body()); got != "shared payload" {
		t.Fatalf("capture = %q, want the full payload", got)
	}
}

// TestBufferingTeeStillSurfacesWriteFailures: the tolerance is specific
// to the detached tee. An ordinary tee must keep reporting a failed
// write, or a real client error would be silently swallowed.
func TestBufferingTeeStillSurfacesWriteFailures(t *testing.T) {
	t.Parallel()
	tee := NewBufferingTee(&brokenWriter{header: make(http.Header)}, 64)
	if _, err := tee.Write([]byte("x")); err == nil {
		t.Fatal("an ordinary tee must report the failed write")
	}
}

// plainWriter is an http.ResponseWriter that is not an http.Flusher, so
// the tee must fall back to flushing nothing.
type plainWriter struct {
	header http.Header
	rec    *httptest.ResponseRecorder
}

func (p *plainWriter) Header() http.Header         { return p.header }
func (p *plainWriter) WriteHeader(code int)        { p.rec.WriteHeader(code) }
func (p *plainWriter) Write(b []byte) (int, error) { return p.rec.Write(b) }

// TestDetachedBufferingTeeMarksTruncation: the detach tolerance must
// not affect the capture limit. A reply past the cap is still marked
// truncated, which is what keeps it out of the shared path.
func TestDetachedBufferingTeeMarksTruncation(t *testing.T) {
	t.Parallel()
	tee := NewDetachedBufferingTee(&plainWriter{header: make(http.Header), rec: httptest.NewRecorder()}, 4)
	if _, err := tee.Write([]byte("abcdefgh")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if !tee.Truncated() {
		t.Fatal("an oversized capture must be marked truncated")
	}
	if got := string(tee.Body()); got != "abcd" {
		t.Fatalf("capture = %q, want the first four bytes", got)
	}
}

// TestDetachedBufferingTeeToleratesANonFlusher: wrapping a writer that
// cannot flush must not panic, and must still capture.
func TestDetachedBufferingTeeToleratesANonFlusher(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	tee := NewDetachedBufferingTee(&plainWriter{header: make(http.Header), rec: rec}, 32)
	tee.WriteHeader(200)
	if _, err := tee.Write([]byte("ok")); err != nil {
		t.Fatalf("write: %v", err)
	}
	tee.Flush()
	if got := string(tee.Body()); got != "ok" {
		t.Fatalf("capture = %q, want %q", got, "ok")
	}
}
