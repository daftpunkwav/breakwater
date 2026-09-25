/**
 * @file sse_writer_failures_test
 * @description Stream encoding against failing writers: named events,
 * and every encode/write error surfaced instead of silently truncating
 * a stream.
 */
package protocol

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

// failingWriter fails every write, the way a client-gone connection
// does.
type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("write: broken pipe") }

func TestWriteEventNamedEventLayout(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	payload := map[string]any{"kind": "delta"}
	if err := WriteEvent(&buf, "response.output_text.delta", payload); err != nil {
		t.Fatalf("write event: %v", err)
	}
	want := "event: response.output_text.delta\ndata: {\"kind\":\"delta\"}\n\n"
	if got := buf.String(); got != want {
		t.Errorf("named event bytes mismatch\n got: %q\nwant: %q", got, want)
	}
}

func TestWriteEventSurfacesWriteFailure(t *testing.T) {
	t.Parallel()
	if err := WriteEvent(failingWriter{}, "error", map[string]any{}); err == nil {
		t.Fatal("a failed event write must surface")
	}
}

func TestWriteDoneSurfacesWriteFailure(t *testing.T) {
	t.Parallel()
	if err := WriteDone(failingWriter{}); err == nil {
		t.Fatal("a failed [DONE] write must surface")
	}
}

func TestWriteDataSurfacesWriteFailure(t *testing.T) {
	t.Parallel()
	if err := WriteData(failingWriter{}, map[string]any{"seq": 1}); err == nil {
		t.Fatal("a failed data write must surface")
	}
}

func TestWriteAbortSurfacesWriteFailure(t *testing.T) {
	t.Parallel()
	if err := WriteAbort(failingWriter{}, CodeUpstreamReset, "reset"); err == nil {
		t.Fatal("a failed abort write must surface")
	}
}

func TestResponsesStreamDeltaIgnoresUnusableChunks(t *testing.T) {
	t.Parallel()
	transcoder := WireFor(FormatOpenAIResponses).Stream()
	var buf bytes.Buffer
	for name, payload := range map[string][]byte{
		"malformed":     []byte("{not json"),
		"no choices":    []byte(`{"id":"x"}`),
		"empty content": []byte(`{"choices":[{"delta":{}}]}`),
	} {
		if err := transcoder.Delta(&buf, payload); err != nil {
			t.Errorf("%s chunk: %v", name, err)
		}
		if buf.Len() != 0 {
			t.Errorf("%s chunk emitted frames: %q", name, buf.String())
		}
	}
}

func TestResponsesStreamDeltaSurfacesWriteFailure(t *testing.T) {
	t.Parallel()
	transcoder := WireFor(FormatOpenAIResponses).Stream()
	if err := transcoder.Delta(failingWriter{}, []byte(`{"choices":[{"delta":{"content":"hi"}}]}`)); err == nil {
		t.Fatal("a failed delta write must surface")
	}
}

func TestAnthropicStreamDeltaSurfacesWriteFailure(t *testing.T) {
	t.Parallel()
	transcoder := WireFor(FormatAnthropicMessages).Stream()
	// The first text piece opens the block; that write fails and the
	// error must propagate instead of streaming a half-open block.
	if err := transcoder.Delta(failingWriter{}, []byte(`{"choices":[{"delta":{"content":"hi"}}]}`)); err == nil {
		t.Fatal("a failed block-start write must surface")
	}
}

func TestAnthropicStreamFinishWithoutContent(t *testing.T) {
	t.Parallel()
	transcoder := WireFor(FormatAnthropicMessages).Stream()
	var buf bytes.Buffer
	if err := transcoder.Start(&buf, "m"); err != nil {
		t.Fatalf("start: %v", err)
	}
	if err := transcoder.Finish(&buf, Usage{}, false); err != nil {
		t.Fatalf("finish: %v", err)
	}
	body := buf.String()
	// No text piece ever arrived: no block may open or close.
	if strings.Contains(body, "content_block_stop") {
		t.Errorf("finish without content closed a block:\n%s", body)
	}
	if !strings.Contains(body, `"stop_reason":"end_turn"`) || !strings.Contains(body, `"output_tokens":0`) {
		t.Errorf("finish without content = %s", body)
	}
}

func TestAnthropicStreamFinishSurfacesWriteFailure(t *testing.T) {
	t.Parallel()
	transcoder := WireFor(FormatAnthropicMessages).Stream()
	var buf bytes.Buffer
	// Open the content block first: the finish sequence must then fail
	// loudly on its very first write (the block stop).
	if err := transcoder.Delta(&buf, []byte(`{"choices":[{"delta":{"content":"hi"}}]}`)); err != nil {
		t.Fatalf("delta: %v", err)
	}
	if err := transcoder.Finish(failingWriter{}, Usage{}, false); err == nil {
		t.Fatal("a failed finish write must surface")
	}
}
