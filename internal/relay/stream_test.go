/**
 * @file stream_test
 * @description Streaming pump behavior tests: byte fidelity of the
 * passthrough pump, including lines longer than the read buffer (the
 * accumulating read fallback) and CRLF line terminators.
 */
package relay

import (
	"errors"
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
