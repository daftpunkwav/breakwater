/**
 * @file pump_transcoder_test
 * @description The translated-stream pump (pumpTranscoded): preamble
 * and terminator calls, per-frame translation, passive usage scraping,
 * and every early-exit error path.
 */
package relay

import (
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/daftpunkwav/breakwater/internal/protocol"
)

// fakeTranscoder records the pump's calls and can fail on demand.
type fakeTranscoder struct {
	startErr  error
	deltaErr  error
	finishErr error

	deltas      []string
	finishUsage protocol.Usage
	finishKnown bool
	started     bool
	finished    bool
}

// streamWriter mirrors the pump's writer parameter type (an anonymous
// write-only interface in the protocol package).
type streamWriter = interface {
	Write(p []byte) (int, error)
}

func (f *fakeTranscoder) Start(w streamWriter, _ string) error {
	f.started = true
	if f.startErr != nil {
		return f.startErr
	}
	_, _ = w.Write([]byte("<start>"))
	return nil
}

func (f *fakeTranscoder) Delta(w streamWriter, payload []byte) error {
	if f.deltaErr != nil {
		return f.deltaErr
	}
	f.deltas = append(f.deltas, string(payload))
	_, _ = w.Write([]byte("<" + string(payload) + ">"))
	return nil
}

func (f *fakeTranscoder) Finish(w streamWriter, usage protocol.Usage, known bool) error {
	f.finished = true
	f.finishUsage, f.finishKnown = usage, known
	if f.finishErr != nil {
		return f.finishErr
	}
	_, _ = w.Write([]byte("<finish>"))
	return nil
}

func (f *fakeTranscoder) Abort(streamWriter, protocol.Code, string) error {
	return nil
}

func TestPumpTranscodedTranslatesAndScrapesUsage(t *testing.T) {
	t.Parallel()
	stream := "data: {\"delta\":\"a\"}\n\n" +
		"data: [DONE]\n\n" +
		"data: {\"usage\":{\"total_tokens\":9}}\n\n"
	tr := &fakeTranscoder{}
	rec := httptest.NewRecorder()

	usage, known, total, err := pumpTranscoded(rec, strings.NewReader(stream), tr, "m1")
	if err != nil {
		t.Fatalf("pump: %v", err)
	}
	if !tr.started || !tr.finished {
		t.Fatalf("lifecycle broken: started=%v finished=%v", tr.started, tr.finished)
	}
	if len(tr.deltas) != 2 || tr.deltas[0] != `{"delta":"a"}` {
		t.Fatalf("deltas = %v, want the two data payloads ([DONE] excluded)", tr.deltas)
	}
	if !known || usage.TotalTokens != 9 {
		t.Fatalf("usage = %+v known=%v, want 9 scraped from a late frame", usage, known)
	}
	if tr.finishUsage.TotalTokens != 9 || !tr.finishKnown {
		t.Fatalf("finish received usage %+v known=%v", tr.finishUsage, tr.finishKnown)
	}
	want := "<start>" +
		"<" + `{"delta":"a"}` + ">" +
		"<" + `{"usage":{"total_tokens":9}}` + ">" +
		"<finish>"
	if got := rec.Body.String(); got != want {
		t.Fatalf("output = %q, want preamble, translated deltas and terminator", got)
	}
	// The count is the rendered bytes actually delivered, not the
	// upstream data lines consumed: the translated wire is what the
	// client received, and StreamBytes meters that.
	if total != int64(len(want)) {
		t.Fatalf("byte count = %d, want the %d rendered bytes", total, len(want))
	}
}

// TestPumpTranscodedRefusesToCompleteATruncatedStream: the [DONE]
// sentinel is the only thing that separates a finished stream from one
// cut short. Byte passthrough needs no such check — the client sees the
// missing terminator for itself — but a translated stream is terminated
// by the gateway, so completing one that never received the sentinel
// would hand the client a short answer dressed as a finished one.
func TestPumpTranscodedRefusesToCompleteATruncatedStream(t *testing.T) {
	t.Parallel()
	stream := "data: {\"delta\":\"a\"}\n\n" +
		"data: {\"usage\":{\"total_tokens\":9}}\n\n"
	tr := &fakeTranscoder{}

	_, _, _, err := pumpTranscoded(httptest.NewRecorder(), strings.NewReader(stream), tr, "m1")
	if !errors.Is(err, errStreamTruncated) {
		t.Fatalf("err = %v, want the truncation error", err)
	}
	if tr.finished {
		t.Fatal("a stream that ended without [DONE] must not be closed as complete")
	}
	if len(tr.deltas) != 2 {
		t.Fatalf("deltas = %v, want both frames translated before the cut", tr.deltas)
	}
}

// TestPumpTranscodedAcceptsSpacelessDataLines: the SSE grammar makes
// the space after "data:" optional; a compatible backend that omits it
// must reach the translator just the same, or its payloads would be
// silently dropped from the client's stream.
func TestPumpTranscodedAcceptsSpacelessDataLines(t *testing.T) {
	t.Parallel()
	stream := "data:{\"delta\":\"a\"}\n\n" +
		"data:{\"usage\":{\"total_tokens\":9}}\n\n" +
		"data:[DONE]\n\n"
	tr := &fakeTranscoder{}
	rec := httptest.NewRecorder()

	usage, known, _, err := pumpTranscoded(rec, strings.NewReader(stream), tr, "m1")
	if err != nil {
		t.Fatalf("pump: %v", err)
	}
	if len(tr.deltas) != 2 || tr.deltas[0] != `{"delta":"a"}` {
		t.Fatalf("deltas = %v, want both spaceless payloads translated", tr.deltas)
	}
	if !known || usage.TotalTokens != 9 || tr.finishUsage.TotalTokens != 9 {
		t.Fatalf("usage = %+v known=%v finish=%+v, want 9 scraped from the spaceless frame", usage, known, tr.finishUsage)
	}
}

func TestPumpTranscodedStartFailureAbortsBeforeFrames(t *testing.T) {
	t.Parallel()
	tr := &fakeTranscoder{startErr: errors.New("cannot write preamble")}
	_, _, _, err := pumpTranscoded(httptest.NewRecorder(), strings.NewReader("data: {}\n\n"), tr, "m1")
	if err == nil {
		t.Fatal("start failure swallowed")
	}
	if len(tr.deltas) != 0 || tr.finished {
		t.Fatalf("pump continued past the failed preamble: deltas=%v finished=%v", tr.deltas, tr.finished)
	}
}

func TestPumpTranscodedDeltaFailureStopsTheStream(t *testing.T) {
	t.Parallel()
	tr := &fakeTranscoder{deltaErr: errors.New("client write failed")}
	_, _, _, err := pumpTranscoded(httptest.NewRecorder(),
		strings.NewReader("data: {\"a\":1}\n\ndata: {\"b\":2}\n\n"), tr, "m1")
	if err == nil {
		t.Fatal("delta failure swallowed")
	}
	if len(tr.deltas) != 0 {
		t.Fatalf("failing delta recorded as delivered: %v", tr.deltas)
	}
}

func TestPumpTranscodedFinishFailurePropagates(t *testing.T) {
	t.Parallel()
	tr := &fakeTranscoder{finishErr: errors.New("terminator write failed")}
	_, _, _, err := pumpTranscoded(httptest.NewRecorder(),
		strings.NewReader("data: {\"a\":1}\n\ndata: [DONE]\n\n"), tr, "m1")
	if err == nil {
		t.Fatal("finish failure swallowed")
	}
	if !tr.finished {
		t.Fatal("finish was never called on EOF")
	}
}

func TestPumpTranscodedMidStreamReadFailure(t *testing.T) {
	t.Parallel()
	tr := &fakeTranscoder{}
	_, _, _, err := pumpTranscoded(httptest.NewRecorder(),
		io.NopCloser(&brokenReader{data: "data: {\"a\":1}\n\n"}), tr, "m1")
	if err == nil {
		t.Fatal("read failure swallowed")
	}
	if tr.finished {
		t.Fatal("finish called on a broken stream: termination belongs to the abort contract")
	}
}
