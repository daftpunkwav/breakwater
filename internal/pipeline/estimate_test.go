/**
 * @file estimate_test
 * @description The token estimate contract: word counting matches
 * strings.Fields exactly (the mock upstream's accounting), the
 * oversized max_tokens clamp holds, and counting never materializes a
 * field slice.
 */
package pipeline

import (
	"strings"
	"testing"

	"github.com/daftpunkwav/breakwater/internal/protocol"
)

func TestCountWordsMatchesFields(t *testing.T) {
	t.Parallel()
	cases := []string{
		"",
		" ",
		"a",
		"hello world",
		"  leading and trailing  ",
		"multiple   spaces\tand\ttabs\nand\nnewlines",
		"separated\u00a0by\u2003nbsp",
		"中文 词组 english mix",
		"\v\fa\vb\fc\r\rd",
	}
	for _, s := range cases {
		want := int64(len(strings.Fields(s)))
		if got := countWords(s); got != want {
			t.Errorf("countWords(%q) = %d, want %d", s, got, want)
		}
	}
}

func TestEstimateTokensClampsDeclaredMaxTokens(t *testing.T) {
	t.Parallel()
	maxTokens := int64(10_000)
	req := protocol.ChatRequest{
		Messages:  []protocol.ChatMessage{{Role: "user", Content: "one two three"}},
		MaxTokens: &maxTokens,
	}
	// The clamp binds the completion reservation to the tenant's
	// per-request cap; the prompt estimate rides on top.
	if got := EstimateTokens(req, 500); got != 503 {
		t.Fatalf("EstimateTokens = %d, want 503 (3 prompt words + clamped 500)", got)
	}
	// Without a tenant cap the declared budget is reserved as-is.
	if got := EstimateTokens(req, 0); got != 10_003 {
		t.Fatalf("EstimateTokens = %d, want 10003", got)
	}
	// No declared max_tokens reserves the default.
	if got := EstimateTokens(protocol.ChatRequest{Messages: req.Messages}, 0); got != 3+DefaultCompletionReserve {
		t.Fatalf("EstimateTokens = %d, want %d", got, 3+DefaultCompletionReserve)
	}
}

func TestEstimatePartialTokensCountsPromptPlusBytes(t *testing.T) {
	t.Parallel()
	req := protocol.ChatRequest{
		Messages: []protocol.ChatMessage{{Role: "user", Content: "one two three four"}},
	}
	if got := EstimatePartialTokens(req, 100); got != 4+25 {
		t.Fatalf("EstimatePartialTokens = %d, want 29 (4 words + 100/4 bytes)", got)
	}
}
