/**
 * @file schema_bench_test
 * @description Cost of the usage scrape: the streaming pump calls
 * ParseUsage once per SSE data frame, so the negative path (a frame
 * without usage) is the hot one and is tracked here.
 */
package protocol

import (
	"testing"
)

// benchChunk is a realistic per-frame chat chunk: a small delta and no
// usage object.
const benchChunk = `{"id":"chatcmpl-1","object":"chat.completion.chunk","created":1,"model":"m","choices":[{"index":0,"delta":{"content":"hello"},"finish_reason":null}]}`

// benchUsageChunk is the one final chunk of a stream that carries usage.
const benchUsageChunk = `{"id":"chatcmpl-1","object":"chat.completion.chunk","choices":[],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}}`

func BenchmarkParseUsageFrameWithoutUsage(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, ok := ParseUsage([]byte(benchChunk)); ok {
			b.Fatal("unexpected usage")
		}
	}
}

func BenchmarkParseUsageFrameWithUsage(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, ok := ParseUsage([]byte(benchUsageChunk)); !ok {
			b.Fatal("expected usage")
		}
	}
}
