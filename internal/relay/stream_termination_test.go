/**
 * @file stream_termination_test
 * @description The honest termination of broken streams:
 * delivered bytes stay sent, one error frame in the client's format
 * closes the stream, the committing candidate owns the abort, and a
 * client that walked away first ends the stream silently.
 */
package relay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/daftpunkwav/breakwater/internal/protocol"
	"github.com/daftpunkwav/breakwater/internal/upstream"
)

// brokenReader yields some bytes then fails, simulating an upstream
// connection reset mid-stream.
type brokenReader struct {
	data string
	off  int
}

func (b *brokenReader) Read(p []byte) (int, error) {
	if b.off >= len(b.data) {
		return 0, errors.New("connection reset by peer")
	}
	n := copy(p, b.data[b.off:])
	b.off += n
	return n, nil
}

func TestStreamAbortTerminatesHonestly(t *testing.T) {
	t.Parallel()
	partial := "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n"
	cand := sseUpstream(&brokenReader{data: partial})
	exec := New(testPolicy(), nil)

	result := execute(t, exec, []upstream.Upstream{cand}, true, "{}")
	if !result.Aborted {
		t.Fatal("result not marked aborted")
	}
	if result.UpstreamID != "s" {
		t.Fatalf("aborted stream served by %q, want s (the committing candidate)", result.UpstreamID)
	}
	body := string(result.Body)
	if !strings.HasPrefix(body, partial) {
		t.Fatalf("delivered bytes lost: %q", body)
	}
	wantTail := "event: error\n" +
		`data: {"error":{"message":"upstream stream failed mid-flight","type":"gateway_error","code":"upstream_reset"}}` +
		"\n\ndata: [DONE]\n\n"
	if !strings.HasSuffix(body, wantTail) {
		t.Fatalf("abort contract violated, tail = %q", body[len(body)-200:])
	}
	if strings.Contains(body, "connection reset by peer") {
		// The raw pump error is a net error whose text carries internal
		// addresses; the client frame stays code-level.
		t.Fatalf("abort frame reflects the raw pump error: %q", body)
	}
}

// TestStreamAbortAfterClientDepartureEndsSilently pins the client-gone
// branch of the committed failure: the pump error lands after the client
// disconnected, so no abort frame is written to the dead connection.
func TestStreamAbortAfterClientDepartureEndsSilently(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	partial := "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n"
	cand := sseUpstream(&cancelOnBreakReader{data: partial, cancel: cancel})
	rec := httptest.NewRecorder()
	exec := New(testPolicy(), nil)

	result := exec.Execute(ctx, Job{
		Model: "test-model", Stream: true,
		Candidates: []upstream.Upstream{cand}, Out: rec,
	})
	if !result.Aborted || !result.ClientGone {
		t.Fatalf("aborted=%v clientGone=%v, want true/true", result.Aborted, result.ClientGone)
	}
	body := rec.Body.String()
	if !strings.HasPrefix(body, partial) {
		t.Fatalf("delivered bytes lost: %q", body)
	}
	if strings.Contains(body, "event: error") {
		t.Fatalf("abort frame written to a disconnected client: %q", body)
	}
}

// cancelOnBreakReader cancels the client context as the stream breaks,
// reproducing the race where the disconnect precedes the pump failure.
type cancelOnBreakReader struct {
	data   string
	off    int
	cancel context.CancelFunc
}

func (b *cancelOnBreakReader) Read(p []byte) (int, error) {
	if b.off >= len(b.data) {
		b.cancel()
		return 0, errors.New("connection reset by peer")
	}
	n := copy(p, b.data[b.off:])
	b.off += n
	return n, nil
}

// TestStreamAbortReusesTheTranscoderInstance locks the translated-wire
// abort contract: the failure frame must carry the same object id the
// preamble introduced — a fresh transcoder would invent a new one.
func TestStreamAbortReusesTheTranscoderInstance(t *testing.T) {
	t.Parallel()
	partial := "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n"
	cand := sseUpstream(&brokenReader{data: partial})
	rec := httptest.NewRecorder()
	exec := New(testPolicy(), nil)
	result := exec.Execute(context.Background(), Job{
		Model:      "test-model",
		Stream:     true,
		Candidates: []upstream.Upstream{cand},
		Wire:       protocol.WireFor(protocol.FormatOpenAIResponses),
		Out:        rec,
	})
	if !result.Aborted {
		t.Fatal("result not marked aborted")
	}
	body := rec.Body.String()
	startID := extractJSONField(t, body, "response.created", "id")
	failID := extractJSONField(t, body, "response.failed", "id")
	if startID == "" || failID == "" {
		t.Fatalf("missing ids: created=%q failed=%q body=%s", startID, failID, body)
	}
	if startID != failID {
		t.Fatalf("abort frame id %q does not match the stream preamble id %q", failID, startID)
	}
}

// TestAbortCodeMapsFailureClasses pins the in-stream error code table:
// timeouts are upstream_timeout, everything else is upstream_reset.
func TestAbortCodeMapsFailureClasses(t *testing.T) {
	t.Parallel()
	timeout := fmt.Errorf("pump: %w", context.DeadlineExceeded)
	if got := abortCode(timeout); got != protocol.CodeUpstreamTimeout {
		t.Fatalf("code = %s, want upstream_timeout", got)
	}
	if got := abortCode(errors.New("connection reset by peer")); got != protocol.CodeUpstreamReset {
		t.Fatalf("code = %s, want upstream_reset", got)
	}
}

// TestStreamAbortFrameStaysCodeLevel pins the abort frame's message
// contract on the timeout classification too: the frame carries the
// code-level phrase, never the raw pump error, whatever broke the
// stream.
func TestStreamAbortFrameStaysCodeLevel(t *testing.T) {
	t.Parallel()
	partial := "data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n"
	cand := sseUpstream(&deadlineReader{data: partial})
	exec := New(testPolicy(), nil)

	result := execute(t, exec, []upstream.Upstream{cand}, true, "{}")
	if !result.Aborted {
		t.Fatal("result not marked aborted")
	}
	body := string(result.Body)
	if !strings.Contains(body, `"code":"upstream_timeout"`) {
		t.Fatalf("abort code = %q, want upstream_timeout", body)
	}
	if !strings.Contains(body, "upstream stream timed out mid-flight") {
		t.Fatalf("abort frame = %q, want the code-level timeout phrase", body)
	}
	if strings.Contains(body, "pump") {
		t.Fatalf("abort frame reflects the raw pump error: %q", body)
	}
}

// deadlineReader yields some bytes then fails with the deadline error,
// simulating an upstream that stops mid-stream at a context deadline.
type deadlineReader struct {
	data string
	off  int
}

func (b *deadlineReader) Read(p []byte) (int, error) {
	if b.off >= len(b.data) {
		return 0, context.DeadlineExceeded
	}
	n := copy(p, b.data[b.off:])
	b.off += n
	return n, nil
}

// extractJSONField pulls one field from the payload of a named SSE
// event in a recorded stream, looking one object level deep.
func extractJSONField(t *testing.T, stream, event, field string) string {
	t.Helper()
	for _, frame := range strings.Split(stream, "\n\n") {
		lines := strings.Split(frame, "\n")
		if len(lines) < 2 || lines[0] != "event: "+event {
			continue
		}
		var payload map[string]any
		if err := json.Unmarshal([]byte(strings.TrimPrefix(lines[1], "data: ")), &payload); err != nil {
			t.Fatalf("event %s payload: %v", event, err)
		}
		if id, ok := payload[field].(string); ok {
			return id
		}
		for _, nested := range payload {
			if obj, ok := nested.(map[string]any); ok {
				if id, ok := obj[field].(string); ok {
					return id
				}
			}
		}
	}
	return ""
}
