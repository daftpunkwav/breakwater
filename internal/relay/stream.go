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
 */
package relay

import (
	"bufio"
	"bytes"
	"io"
	"net/http"

	"github.com/daftpunkwav/breakwater/internal/protocol"
)

// dataPrefix marks an SSE data line.
const dataPrefix = "data: "

// pumpStream copies the SSE stream from body to out line by line,
// flushing at event boundaries, and returns the usage object when the
// stream carried one. A returned error means the stream broke mid-flight
// — the caller owns the abort contract.
func pumpStream(out http.ResponseWriter, body io.Reader) (protocol.Usage, bool, error) {
	reader := bufio.NewReader(body)
	flusher, flushes := out.(http.Flusher)
	var usage protocol.Usage
	usageKnown := false

	for {
		line, readErr := reader.ReadString('\n')
		if len(line) > 0 {
			trimmed := trimEOL(line)
			if payload, isData := scrapePayload([]byte(trimmed)); isData {
				if u, ok := protocol.ParseUsage(payload); ok {
					usage, usageKnown = u, true
				}
			}
			if _, writeErr := io.WriteString(out, trimmed+"\n"); writeErr != nil {
				return usage, usageKnown, writeErr
			}
			// A blank line closes one SSE event: the client-visible
			// boundary to flush at.
			if trimmed == "" && flushes {
				flusher.Flush()
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				return usage, usageKnown, nil
			}
			return usage, usageKnown, readErr
		}
	}
}

// scrapePayload extracts the JSON payload of an SSE data line; ok is
// false for every other line (comments, event names, blank lines),
// which pass through untouched.
func scrapePayload(line []byte) (payload []byte, ok bool) {
	rest, found := bytes.CutPrefix(line, []byte(dataPrefix))
	return rest, found
}

// trimEOL strips one trailing line terminator (\n or \r\n).
func trimEOL(line string) string {
	line = trimSuffixByte(line, '\n')
	return trimSuffixByte(line, '\r')
}

func trimSuffixByte(s string, b byte) string {
	if len(s) > 0 && s[len(s)-1] == b {
		return s[:len(s)-1]
	}
	return s
}
