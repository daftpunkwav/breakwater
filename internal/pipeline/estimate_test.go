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

func TestEstimateTokensReservesDefaultForNonPositiveMaxTokens(t *testing.T) {
	t.Parallel()
	messages := []protocol.ChatMessage{{Role: "user", Content: "one two three"}}
	// A declared budget that is zero or negative cannot be honored; the
	// safe default reserves instead of reserving nothing.
	zero, negative := int64(0), int64(-5)
	for _, declared := range []*int64{nil, &zero, &negative} {
		req := protocol.ChatRequest{Messages: messages, MaxTokens: declared}
		if got := EstimateTokens(req, 0); got != 3+DefaultCompletionReserve {
			t.Fatalf("EstimateTokens(declared=%v) = %d, want %d", declared, got, 3+DefaultCompletionReserve)
		}
	}
}

func TestEstimateTokensFloorsEmptyPromptAtOne(t *testing.T) {
	t.Parallel()
	// A request without messages still reserves one prompt token, so a
	// zero-cost reservation can never ride through.
	if got := EstimateTokens(protocol.ChatRequest{}, 0); got != 1+DefaultCompletionReserve {
		t.Fatalf("EstimateTokens = %d, want %d", got, 1+DefaultCompletionReserve)
	}
}

func TestEstimateTokensClampBoundaryKeepsEqualBudget(t *testing.T) {
	t.Parallel()
	maxTokens := int64(500)
	req := protocol.ChatRequest{
		Messages:  []protocol.ChatMessage{{Role: "user", Content: "one two three"}},
		MaxTokens: &maxTokens,
	}
	// A declared budget equal to the tenant cap is kept as-is; only
	// strictly larger budgets clamp.
	if got := EstimateTokens(req, 500); got != 503 {
		t.Fatalf("EstimateTokens = %d, want 503 (3 prompt words + 500)", got)
	}
}

func TestCarrierEstimatesScanOnceAndMatchFreeForms(t *testing.T) {
	t.Parallel()
	maxTokens := int64(10_000)
	chat := protocol.ChatRequest{
		Model:     "mock-model",
		Messages:  []protocol.ChatMessage{{Role: "user", Content: "one two three"}, {Role: "assistant", Content: "four five"}},
		MaxTokens: &maxTokens,
	}
	carrier := &Carrier{Chat: chat}

	// The carrier forms agree with the free forms they mirror.
	if got, want := carrier.PromptTokens(), PromptTokens(chat); got != want {
		t.Fatalf("carrier PromptTokens = %d, want %d", got, want)
	}
	if got, want := carrier.ReserveTokens(500), EstimateTokens(chat, 500); got != want {
		t.Fatalf("carrier ReserveTokens = %d, want %d", got, want)
	}
	if got, want := carrier.EstimatePartialTokens(100), EstimatePartialTokens(chat, 100); got != want {
		t.Fatalf("carrier EstimatePartialTokens = %d, want %d", got, want)
	}

	// Mutating the caller's chat afterwards must not move the cached
	// estimate: one scan per request, by design — the stages downstream
	// of ingest read the same request.
	chat.Messages[0].Content = "completely different text"
	if got := carrier.PromptTokens(); got != 5 {
		t.Fatalf("cached PromptTokens = %d, want the cached 5", got)
	}
}
