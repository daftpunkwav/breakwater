/**
 * @file sse_test
 * @description Contract tests for the frozen SSE stream protocol: the
 * byte layout of the abort sequence is pinned exactly as specified, and
 * the error code enum is locked.
 */
package protocol

import (
	"bytes"
	"testing"
)

// TestWriteAbortContract pins the frozen abort sequence byte for byte:
// one error event, then the [DONE] sentinel, separated by blank lines.
func TestWriteAbortContract(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	err := WriteAbort(&buf, CodeUpstreamReset, "upstream connection reset mid-stream")
	if err != nil {
		t.Fatalf("write abort: %v", err)
	}

	want := "event: error\n" +
		`data: {"error":{"message":"upstream connection reset mid-stream","type":"gateway_error","code":"upstream_reset"}}` +
		"\n\n" +
		"data: [DONE]\n\n"
	if got := buf.String(); got != want {
		t.Errorf("abort bytes mismatch\n got: %q\nwant: %q", got, want)
	}
}

// TestWriteAbortAllCodes exercises every frozen code through the abort
// path so the enum cannot drift silently.
func TestWriteAbortAllCodes(t *testing.T) {
	t.Parallel()

	for _, code := range []Code{CodeUpstreamReset, CodeUpstreamTimeout, CodeBudgetExhausted} {
		var buf bytes.Buffer
		if err := WriteAbort(&buf, code, "m"); err != nil {
			t.Fatalf("write abort %s: %v", code, err)
		}
		want := `data: {"error":{"message":"m","type":"gateway_error","code":"` + string(code) + `"}}`
		if !bytes.Contains(buf.Bytes(), []byte(want)) {
			t.Errorf("abort stream for %s missing payload %s, got %q", code, want, buf.String())
		}
		if !bytes.HasSuffix(buf.Bytes(), []byte("data: [DONE]\n\n")) {
			t.Errorf("abort stream for %s does not end with [DONE]: %q", code, buf.String())
		}
	}
}

func TestWriteDataAndDone(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	payload := struct {
		Seq int `json:"seq"`
	}{Seq: 7}
	if err := WriteData(&buf, payload); err != nil {
		t.Fatalf("write data: %v", err)
	}
	if err := WriteDone(&buf); err != nil {
		t.Fatalf("write done: %v", err)
	}

	want := "data: {\"seq\":7}\n\ndata: [DONE]\n\n"
	if got := buf.String(); got != want {
		t.Errorf("stream bytes mismatch\n got: %q\nwant: %q", got, want)
	}
}

func TestWriteDataRejectsUnencodablePayload(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	err := WriteData(&buf, func() {})
	if err == nil {
		t.Fatal("expected error for unencodable payload")
	}
	if buf.Len() != 0 {
		t.Errorf("failed encode must not write bytes, got %q", buf.String())
	}
}
