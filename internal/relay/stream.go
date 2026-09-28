/**
 * @file stream
 * @description The streaming passthrough pump: relays SSE bytes from an
 * upstream body to the client while passively scraping token usage.
 *
 * Responsibilities:
 * - Move bytes chunk by chunk with a fixed buffer, flushing per event,
 *   never accumulating the body in memory
 * - Scrape the usage object out of data events for settlement, without
 *   re-encoding the stream
 * - Nothing else: abort rendering and retry decisions live with their
 *   owners; a pump error just means "the stream broke"
 *
 * Allocation discipline: one SSE stream carries thousands of frames, so
 * the pump reads lines through bufio ReadSlice (no per-line copy for
 * lines that fit the buffer; over-long lines fall back to an
 * accumulating read) and reuses one outgoing buffer instead of building
 * a fresh string per frame.
 */
package relay

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net/http"

	"github.com/daftpunkwav/breakwater/internal/protocol"
)

// dataPayload extracts an SSE data line's payload: the "data:" field
// name plus at most one separating space, per the SSE grammar (a
// writer that skips the space is equally canonical). ok is false for
// any other line kind.
func dataPayload(line []byte) (payload []byte, ok bool) {
	rest, isData := bytes.CutPrefix(line, []byte("data:"))
	if !isData {
		return nil, false
	}
	return bytes.TrimPrefix(rest, []byte(" ")), true
}

// donePayload is the [DONE] token carried inside the canonical wire's
// terminator data line (the full line form lives in the protocol
// package, which owns the wire format).
const donePayload = "[DONE]"

// errStreamTruncated reports a stream the upstream ended without the
// [DONE] terminator. The reply is incomplete, and the translated wires
// would otherwise synthesize a normal completion the provider never
// sent.
var errStreamTruncated = errors.New("relay: upstream stream ended without the [DONE] terminator")

// pumpTranscoded feeds the upstream SSE sequence through a stream
// transcoder: the preamble opens the exchange, every data frame is
// translated, the terminator (or the abort sequence) closes it. Usage
// is still scraped passively for settlement.
//
// The [DONE] sentinel is what separates a finished stream from a
// truncated one. Byte passthrough needs no such distinction — the
// client sees the missing terminator for itself — but a translated
// stream is terminated by the gateway, so without this check a cut
// stream would be dressed up as a complete answer.
func pumpTranscoded(out http.ResponseWriter, body io.Reader, transcoder protocol.StreamTranscoder, model string) (protocol.Usage, bool, int64, error) {
	reader := bufio.NewReader(body)
	flusher, flushes := out.(http.Flusher)
	var usage protocol.Usage
	usageKnown := false
	var total int64
	doneSeen := false

	flush := func() {
		if flushes {
			flusher.Flush()
		}
	}

	if err := transcoder.Start(out, model); err != nil {
		return usage, usageKnown, total, err
	}
	flush()

	for {
		line, readErr := readLine(reader)
		if len(line) > 0 {
			trimmed := trimEOL(line)
			if payload, isData := dataPayload(trimmed); isData {
				if string(payload) != donePayload {
					if u, ok := protocol.ParseUsage(payload); ok {
						usage, usageKnown = u, true
					}
					if err := transcoder.Delta(out, payload); err != nil {
						return usage, usageKnown, total, err
					}
				} else {
					doneSeen = true
				}
				total += int64(len(trimmed)) + 1
				flush()
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				if !doneSeen {
					return usage, usageKnown, total, errStreamTruncated
				}
				return usage, usageKnown, total, transcoder.Finish(out, usage, usageKnown)
			}
			return usage, usageKnown, total, readErr
		}
	}
}

// pumpStream copies the SSE stream from body to out line by line,
// flushing at event boundaries, and returns the usage object when the
// stream carried one plus the total byte count it passed through. A
// returned error means the stream broke mid-flight — the caller owns
// the abort contract.
//
// Bytes are copied verbatim, but the [DONE] sentinel is still tracked.
// The client can see the missing terminator for itself; the gateway
// cannot, and a truncated stream is upstream evidence the breaker and
// the latency tracker must see identically for every client format. A
// non-SSE body (no data line at all) has no sentinel to wait for, so
// plain EOF is still a success.
func pumpStream(out http.ResponseWriter, body io.Reader) (protocol.Usage, bool, int64, error) {
	reader := bufio.NewReader(body)
	flusher, flushes := out.(http.Flusher)
	var usage protocol.Usage
	usageKnown := false
	var total int64
	// outBuf carries one outgoing line and is reused across the pump;
	// building a fresh string per frame would put one allocation per
	// SSE line on the hot path.
	outBuf := make([]byte, 0, 512)
	doneSeen := false
	sawData := false

	for {
		line, readErr := readLine(reader)
		if len(line) > 0 {
			trimmed := trimEOL(line)
			if payload, isData := dataPayload(trimmed); isData {
				sawData = true
				if string(payload) == donePayload {
					doneSeen = true
				}
				if u, ok := protocol.ParseUsage(payload); ok {
					usage, usageKnown = u, true
				}
			}
			outBuf = append(outBuf[:0], trimmed...)
			outBuf = append(outBuf, '\n')
			if _, writeErr := out.Write(outBuf); writeErr != nil {
				return usage, usageKnown, total, writeErr
			}
			total += int64(len(trimmed)) + 1
			// A blank line closes one SSE event: the client-visible
			// boundary to flush at.
			if len(trimmed) == 0 && flushes {
				flusher.Flush()
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				if sawData && !doneSeen {
					return usage, usageKnown, total, errStreamTruncated
				}
				return usage, usageKnown, total, nil
			}
			return usage, usageKnown, total, readErr
		}
	}
}

// activityReader feeds a stream body through a progress callback:
// every read that produced bytes counts as activity. The idle watchdog
// reads it as "the upstream spoke" and holds off; silence lets the
// watchdog run out.
type activityReader struct {
	reader   io.Reader
	activity func()
}

func (r *activityReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 && r.activity != nil {
		r.activity()
	}
	return n, err
}

// readLine returns the next '\n'-terminated line without copying for
// lines that fit bufio's buffer. A line longer than the buffer is
// accumulated by falling back to the allocating read. The returned
// slice is valid only until the next read.
func readLine(reader *bufio.Reader) ([]byte, error) {
	line, err := reader.ReadSlice('\n')
	if err != bufio.ErrBufferFull {
		return line, err
	}
	whole := append([]byte(nil), line...)
	for err == bufio.ErrBufferFull {
		line, err = reader.ReadSlice('\n')
		whole = append(whole, line...)
	}
	return whole, err
}

// trimEOL strips one trailing line terminator (\n or \r\n) from a
// buffered line.
func trimEOL(line []byte) []byte {
	if n := len(line); n > 0 && line[n-1] == '\n' {
		line = line[:n-1]
	}
	if n := len(line); n > 0 && line[n-1] == '\r' {
		line = line[:n-1]
	}
	return line
}
