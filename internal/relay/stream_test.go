/**
 * @file stream_test
 * @description Streaming pump behavior tests: byte fidelity of the
 * passthrough pump, including lines longer than the read buffer (the
 * accumulating read fallback), CRLF line terminators, and the bound
 * that fallback puts on an upstream that never terminates a line.
 */
package relay

import (
	"bufio"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestPumpStreamPreservesBytesAcrossLineShapes locks the passthrough
// fidelity of pumpStream for the line shapes real upstreams emit: short
// frames, frames whose single data line exceeds the read buffer (the
// ReadSlice fallback must reassemble them without dropping or
// duplicating bytes), and CRLF terminators (normalized to LF on the way
// out, since the pump re-emits line by line).
func TestPumpStreamPreservesBytesAcrossLineShapes(t *testing.T) {
	t.Parallel()
	longPayload := strings.Repeat("x", 3*4096) // three read buffers long

	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "short frames pass through verbatim",
			in:   "data: {\"a\":1}\n\ndata: [DONE]\n\n",
			want: "data: {\"a\":1}\n\ndata: [DONE]\n\n",
		},
		{
			name: "line longer than the read buffer survives whole",
			in:   "data: {\"choices\":[{\"delta\":{\"content\":\"" + longPayload + "\"}}]}\n\ndata: [DONE]\n\n",
			want: "data: {\"choices\":[{\"delta\":{\"content\":\"" + longPayload + "\"}}]}\n\ndata: [DONE]\n\n",
		},
		{
			name: "crlf terminators normalize to lf",
			in:   "data: {\"a\":1}\r\n\r\ndata: [DONE]\r\n\r\n",
			want: "data: {\"a\":1}\n\ndata: [DONE]\n\n",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			_, _, total, err := pumpStream(rec, strings.NewReader(tc.in))
			if err != nil {
				t.Fatalf("pump: %v", err)
			}
			if total != int64(len(tc.want)) {
				t.Fatalf("byte count = %d, want %d", total, len(tc.want))
			}
			if got := rec.Body.String(); got != tc.want {
				t.Fatalf("stream bytes altered\n got %d bytes\nwant %d bytes", len(got), len(tc.want))
			}
		})
	}
}

// failingWriter rejects every write, simulating a client connection
// that died under the passthrough pump.
type failingWriter struct {
	header http.Header
}

func (f failingWriter) Header() http.Header { return f.header }

func (failingWriter) Write([]byte) (int, error) {
	return 0, errors.New("client write failed")
}

func (failingWriter) WriteHeader(int) {}

// TestPumpStreamWriteFailureStopsThePump pins the pump's failure path:
// a broken client write ends the pump with the error instead of
// spinning on the remaining upstream bytes.
func TestPumpStreamWriteFailureStopsThePump(t *testing.T) {
	t.Parallel()
	stream := "data: {\"a\":1}\n\ndata: {\"b\":2}\n\ndata: [DONE]\n\n"
	usage, known, total, err := pumpStream(failingWriter{}, strings.NewReader(stream))
	if err == nil {
		t.Fatal("write failure swallowed")
	}
	if known {
		t.Fatalf("usage = %+v, want unknown: the failing frame carried none", usage)
	}
	if total != 0 {
		t.Fatalf("byte count = %d, want 0: nothing was delivered", total)
	}
}

// TestPumpStreamFlagsATruncatedStreamAsUpstreamEvidence pins the
// passthrough pump's sentinel contract: data frames that end without
// [DONE] surface the truncation error, so the breaker and the latency
// tracker see a cut stream as upstream evidence instead of a complete
// answer. The bytes forwarded before the cut still count as delivered —
// the client received them, and settlement must say so.
func TestPumpStreamFlagsATruncatedStreamAsUpstreamEvidence(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		in      string
		wantErr bool
	}{
		{
			name:    "data frames without the [DONE] sentinel are truncated",
			in:      "data: {\"choices\":[{\"delta\":{\"content\":\"he\"}}]}\n\n",
			wantErr: true,
		},
		{
			name:    "the [DONE] sentinel completes the stream",
			in:      "data: {\"a\":1}\n\ndata: [DONE]\n\n",
			wantErr: false,
		},
		{
			name:    "a body without any data line has no sentinel to wait for",
			in:      ": keepalive comment\n\n",
			wantErr: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			_, _, total, err := pumpStream(rec, strings.NewReader(tc.in))
			if tc.wantErr && err == nil {
				t.Fatalf("err = nil, want the truncation error: a cut stream must not read as complete")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("err = %v, want nil", err)
			}
			if total != int64(rec.Body.Len()) {
				t.Fatalf("byte count = %d, want the %d bytes actually forwarded", total, rec.Body.Len())
			}
		})
	}
}

// endlessLines is a reader that never emits a line terminator: it fills
// every read with payload bytes. The pump's accumulator is bounded only
// by readLine, so this is what an upstream sending one unbounded line
// looks like — progress without a terminator, which is also why the
// idle watchdog cannot catch it.
type endlessLines struct{}

func (endlessLines) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	return len(p), nil
}

// TestReadLineStopsAtTheLineCap pins the cap on the pump's
// over-long-line fallback. An upstream that streams without ever
// sending '\n' used to grow the accumulator until the process died:
// every read still counted as stream activity, so the idle watchdog
// stayed silent and only the stream ceiling bounded it, in time rather
// than in memory.
func TestReadLineStopsAtTheLineCap(t *testing.T) {
	t.Parallel()
	// Two read buffers past the cap, so the refusal comes from the cap
	// and not from the input running out.
	overflow := int64(maxStreamLineBytes) + 2*4096
	reader := bufio.NewReader(io.LimitReader(endlessLines{}, overflow))

	line, err := readLine(reader)
	if !errors.Is(err, errStreamLineTooLong) {
		t.Fatalf("err = %v, want %v: an unterminated line must be cut at the cap", err, errStreamLineTooLong)
	}
	if line != nil {
		t.Fatalf("line = %d bytes, want none: a cut line is never a partial reply", len(line))
	}
}

// TestReadLineKeepsLinesUnderTheCap guards the other side of the cap:
// a line the buffered path would accept in full is still reassembled
// whole, so the bound rejects only what the line cap rejects.
func TestReadLineKeepsLinesUnderTheCap(t *testing.T) {
	t.Parallel()
	const size = maxStreamLineBytes / 2
	body := "data: " + strings.Repeat("x", size) + "\n"
	reader := bufio.NewReader(strings.NewReader(body))

	line, err := readLine(reader)
	if err != nil {
		t.Fatalf("readLine: %v", err)
	}
	if string(line) != body {
		t.Fatalf("line = %d bytes, want the %d-byte line whole", len(line), len(body))
	}
}

// TestPumpStreamReportsAnUnterminatedLineAsUpstreamFault checks the cap
// is an upstream fault end to end: the pump stops, the client sees an
// honest abort frame rather than a stream that never terminates, and
// nothing of the cut line reaches it.
func TestPumpStreamReportsAnUnterminatedLineAsUpstreamFault(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	_, known, total, err := pumpStream(rec, io.LimitReader(endlessLines{}, maxStreamLineBytes+2*4096))
	if !errors.Is(err, errStreamLineTooLong) {
		t.Fatalf("err = %v, want %v", err, errStreamLineTooLong)
	}
	if known {
		t.Fatal("usage reported: a cut line carries no usage object")
	}
	if total != 0 {
		t.Fatalf("byte count = %d, want 0: the cut line was never delivered", total)
	}
	if got := rec.Body.String(); got != "" {
		t.Fatalf("client received %d bytes of an unterminated line", len(got))
	}
}
