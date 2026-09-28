/**
 * @file estimate_bench_test
 * @description Cost of the per-request token estimation the governance
 * stages perform: the limiter's reservation estimate plus the
 * inference handler's prompt-only pre-filter, back to back on one
 * request — the sequence every governed request pays.
 */
package pipeline

import (
	"fmt"
	"strings"
	"testing"

	"github.com/daftpunkwav/breakwater/internal/protocol"
)

// benchChat builds a chat request of n messages with ~500-char bodies,
// the shape a long-conversation client sends.
func benchChat(n int) protocol.ChatRequest {
	chat := protocol.ChatRequest{Model: "mock-model"}
	content := strings.Repeat("word ", 100)
	for i := 0; i < n; i++ {
		chat.Messages = append(chat.Messages, protocol.ChatMessage{
			Role: "user", Content: fmt.Sprintf("%s m%d", content, i),
		})
	}
	return chat
}

// BenchmarkRequestTokenEstimate measures the estimation work of one
// governed request: the limiter stage's reservation estimate followed
// by the inference handler's prompt-only context check, and the
// partial-stream fallback a usage-less stream pays after them.
func BenchmarkRequestTokenEstimate(b *testing.B) {
	chat := benchChat(20)
	maxRequestTokens := int64(4096)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		carrier := &Carrier{Chat: chat}
		// The limiter stage:
		tokens := carrier.ReserveTokens(maxRequestTokens)
		carrier.Tokens = tokens
		// The inference handler's context pre-filter:
		input := carrier.PromptTokens()
		// The settlement fallback on a stream that ended without usage:
		_ = carrier.EstimatePartialTokens(4096)
		_, _, _ = tokens, input, carrier.Tokens
	}
}
