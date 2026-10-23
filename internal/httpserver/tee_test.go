/**
 * @file tee_test
 * @description Tee mechanics: capture fidelity, mode behavior, single
 * WriteHeader, flush recording, and the write deadline that bounds one
 * blocked write to a slow client.
 */
package httpserver

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"
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

// TestDetachedTeeReportsFullWriteOnAbsorbedFailure pins the write
// contract of the detached tee: an absorbed failure must read as full
// success, because an io.Writer that returns fewer bytes than it was
// handed must return an error — and not surfacing the error is the
// whole point of the detached mode. A short nil-error report would
// break every caller that trusts the io.Writer contract.
func TestDetachedTeeReportsFullWriteOnAbsorbedFailure(t *testing.T) {
	t.Parallel()
	tee := NewDetachedBufferingTee(&brokenWriter{header: make(http.Header)}, 64)

	n, err := tee.Write([]byte("shared payload"))
	if err != nil {
		t.Fatalf("write surfaced the client's disconnect: %v", err)
	}
	if n != len("shared payload") {
		t.Fatalf("write = %d, want %d (short report with nil error)", n, len("shared payload"))
	}
	if got := tee.BytesWritten(); got != int64(len("shared payload")) {
		t.Fatalf("bytes written = %d, want %d (the count must tell the same story as the report)", got, len("shared payload"))
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

// errNoDeadline reports a write that reached the client without an
// armed write deadline: the tee must arm one before every write.
var errNoDeadline = errors.New("write without an armed deadline")

// blockingWriter stands in for a client that stopped reading: its
// Write blocks until the armed write deadline passes, then fails the
// way a socket write does.
type blockingWriter struct {
	header http.Header

	mu       sync.Mutex
	deadline time.Time
}

func (b *blockingWriter) Header() http.Header  { return b.header }
func (b *blockingWriter) WriteHeader(code int) {}

func (b *blockingWriter) SetWriteDeadline(t time.Time) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.deadline = t
	return nil
}

func (b *blockingWriter) Write(p []byte) (int, error) {
	b.mu.Lock()
	deadline := b.deadline
	b.mu.Unlock()
	if deadline.IsZero() {
		return 0, errNoDeadline
	}
	<-time.After(time.Until(deadline))
	return 0, os.ErrDeadlineExceeded
}

// armedDeadline reports the deadline the last write saw.
func (b *blockingWriter) armedDeadline() time.Time {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.deadline
}

// TestTeeBoundsABlockedWrite: a write that blocks on a slow client
// must not pin the request's governance resources — the quota lease,
// the limiter reservation, the bulkhead slot and the circuit permit are
// released only when the write returns. The tee arms the connection's
// write deadline, so the blocked write gives up on its own.
func TestTeeBoundsABlockedWrite(t *testing.T) {
	t.Parallel()
	inner := &blockingWriter{header: make(http.Header)}
	tee := NewCountingTee(inner)
	tee.writeTimeout = 20 * time.Millisecond

	start := time.Now()
	_, err := tee.Write([]byte("x"))
	if err == nil {
		t.Fatal("a blocked write must fail once the deadline passes")
	}
	if errors.Is(err, errNoDeadline) {
		t.Fatal("the tee wrote without arming the write deadline")
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("the blocked write took %s, want the deadline to cut it", elapsed)
	}
}

// TestTeeRearmsTheWriteDeadlinePerWrite: a long-lived stream writes
// many times over, so the deadline must be armed again before every
// write. A deadline set once would cut the stream at its first idle
// moment.
func TestTeeRearmsTheWriteDeadlinePerWrite(t *testing.T) {
	t.Parallel()
	inner := &blockingWriter{header: make(http.Header)}
	tee := NewCountingTee(inner)
	tee.writeTimeout = 20 * time.Millisecond

	if _, err := tee.Write([]byte("a")); err == nil {
		t.Fatal("the first write must fail on the deadline")
	}
	first := inner.armedDeadline()
	time.Sleep(5 * time.Millisecond)
	if _, err := tee.Write([]byte("b")); err == nil {
		t.Fatal("the second write must fail on the deadline")
	}
	if second := inner.armedDeadline(); !second.After(first) {
		t.Fatalf("the deadline was not re-armed: first %s, second %s", first, second)
	}
}

// TestTeeWritesToAWriterWithoutDeadlines: a writer that cannot carry a
// deadline — a recorder, a plain writer — is written to as before.
func TestTeeWritesToAWriterWithoutDeadlines(t *testing.T) {
	t.Parallel()
	inner := httptest.NewRecorder()
	tee := NewCountingTee(inner)
	if _, err := tee.Write([]byte("ok")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if got := inner.Body.String(); got != "ok" {
		t.Fatalf("forwarded = %q, want %q", got, "ok")
	}
}

// TestTeeWithoutAWriteTimeoutWritesAsBefore: the arming is a transport
// bound, not a contract every write has to carry. A tee without a
// timeout writes the way it always did.
func TestTeeWithoutAWriteTimeoutWritesAsBefore(t *testing.T) {
	t.Parallel()
	inner := httptest.NewRecorder()
	tee := NewCountingTee(inner)
	tee.writeTimeout = 0
	if _, err := tee.Write([]byte("ok")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if got := inner.Body.String(); got != "ok" {
		t.Fatalf("forwarded = %q, want %q", got, "ok")
	}
}

// TestTeeUnwrapsToTheWriterItWraps: the deadline has to reach the
// connection through a chain of tees — the cache stage's tee wraps the
// observation stage's tee — so the tee exposes the writer it wraps and
// a response controller reaches the deadline carrier through it.
func TestTeeUnwrapsToTheWriterItWraps(t *testing.T) {
	t.Parallel()
	inner := &blockingWriter{header: make(http.Header)}
	outer := NewCountingTee(inner)
	outermost := NewCountingTee(outer)

	if got := outermost.Unwrap(); got != http.ResponseWriter(outer) {
		t.Fatal("the tee must expose the writer it wraps")
	}
	if err := http.NewResponseController(outermost).SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatalf("a controller must reach the deadline carrier through the tee chain: %v", err)
	}
	if inner.armedDeadline().IsZero() {
		t.Fatal("the deadline did not reach the connection through the tee chain")
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
