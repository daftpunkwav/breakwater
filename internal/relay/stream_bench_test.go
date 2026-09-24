/**
 * @file stream_bench_test
 * @description Allocation and speed baseline of the streaming pumps: the
 * per-line cost of the passthrough path is the gateway's CPU floor for
 * SSE relay, so it is tracked here.
 */
package relay

import (
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/daftpunkwav/breakwater/internal/protocol"
)

// discardWriter drops every write without allocating, isolating the
// pump's own allocation profile from the response writer's.
type discardWriter struct{}

func (discardWriter) Header() http.Header         { return nil }
func (discardWriter) WriteHeader(int)             {}
func (discardWriter) Write(p []byte) (int, error) { return len(p), nil }

// sseBody builds a realistic chunk stream: chunks data frames of roughly
// 250 bytes each, terminated by the usage chunk and [DONE].
func sseBody(chunks int) string {
	var b strings.Builder
	payload := strings.Repeat("x", 200)
	for i := 0; i < chunks; i++ {
		fmt.Fprintf(&b, "data: {\"choices\":[{\"delta\":{\"content\":\"%s\"}}]}\n\n", payload)
	}
	b.WriteString("data: {\"choices\":[],\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":2,\"total_tokens\":5}}\n\n")
	b.WriteString("data: [DONE]\n\n")
	return b.String()
}

func BenchmarkPumpStream(b *testing.B) {
	body := sseBody(1000)
	b.SetBytes(int64(len(body)))
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, _, _, err := pumpStream(discardWriter{}, strings.NewReader(body)); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPumpTranscoded(b *testing.B) {
	body := sseBody(1000)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		transcoder := protocol.WireFor(protocol.FormatAnthropicMessages).Stream()
		if _, _, _, err := pumpTranscoded(discardWriter{}, strings.NewReader(body), transcoder, "m"); err != nil {
			b.Fatal(err)
		}
	}
}
