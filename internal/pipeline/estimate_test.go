/**
 * @file estimate_test
 * @description The token estimate contract: the prompt is measured by
 * the shared content measure, the oversized max_tokens clamp holds,
 * and the estimate saturates instead of wrapping. Every case drives
 * the Carrier face — the only entry the pipeline stages use.
 */
package pipeline

import (
	"math"
	"strings"
	"testing"

	"github.com/daftpunkwav/breakwater/internal/protocol"
)

// estimateOf prices req through the carrier's reservation entry, the
// path the limiter and quota stages take.
func estimateOf(req protocol.ChatRequest, maxRequestTokens int64) int64 {
	return (&Carrier{Chat: req}).ReserveTokens(maxRequestTokens)
}

// promptOf prices req's prompt through the carrier's cached scan.
func promptOf(req protocol.ChatRequest) int64 {
	return (&Carrier{Chat: req}).PromptTokens()
}

// TestEstimateTokensPricesAWhitespaceFreePrompt: a prompt without
// whitespace carries no word for the word rule to count, so it used to
// price as a single token however large it was — and a prompt-sized
// spend that prices as one token escapes the TPM bucket and the
// monthly quota, the two controls that exist to bound token spend.
func TestEstimateTokensPricesAWhitespaceFreePrompt(t *testing.T) {
	t.Parallel()
	content := strings.Repeat("中文字幕", 100)
	req := protocol.ChatRequest{
		Messages: []protocol.ChatMessage{{Role: "user", Content: content}},
	}
	want := int64(len(content))/protocol.BytesPerToken + DefaultCompletionReserve
	if got := estimateOf(req, 0); got != want {
		t.Fatalf("EstimateTokens = %d, want %d", got, want)
	}
	if got := promptOf(req); got != want-DefaultCompletionReserve {
		t.Fatalf("PromptTokens = %d, want %d", got, want-DefaultCompletionReserve)
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
	if got := estimateOf(req, 500); got != 503 {
		t.Fatalf("EstimateTokens = %d, want 503 (3 prompt words + clamped 500)", got)
	}
	// Without a tenant cap the declared budget is reserved as-is.
	if got := estimateOf(req, 0); got != 10_003 {
		t.Fatalf("EstimateTokens = %d, want 10003", got)
	}
	// No declared max_tokens reserves the default.
	if got := estimateOf(protocol.ChatRequest{Messages: req.Messages}, 0); got != 3+DefaultCompletionReserve {
		t.Fatalf("EstimateTokens = %d, want %d", got, 3+DefaultCompletionReserve)
	}
}

func TestEstimatePartialTokensCountsPromptPlusBytes(t *testing.T) {
	t.Parallel()
	req := protocol.ChatRequest{
		Messages: []protocol.ChatMessage{{Role: "user", Content: "one two three four"}},
	}
	if got := (&Carrier{Chat: req}).EstimatePartialTokens(100); got != 4+25 {
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
		if got := estimateOf(req, 0); got != 3+DefaultCompletionReserve {
			t.Fatalf("EstimateTokens(declared=%v) = %d, want %d", declared, got, 3+DefaultCompletionReserve)
		}
	}
}

func TestEstimateTokensFloorsEmptyPromptAtOne(t *testing.T) {
	t.Parallel()
	// A request without messages still reserves one prompt token, so a
	// zero-cost reservation can never ride through.
	if got := estimateOf(protocol.ChatRequest{}, 0); got != 1+DefaultCompletionReserve {
		t.Fatalf("EstimateTokens = %d, want %d", got, 1+DefaultCompletionReserve)
	}
}

// TestEstimateTokensSaturatesHugeDeclaredBudget: a client-declared
// max_tokens near the int64 ceiling against a tier without its own
// clamp must saturate the estimate, not wrap it negative — a negative
// reservation reads to the limiter as affordable and to the ledger as
// a balance credit.
func TestEstimateTokensSaturatesHugeDeclaredBudget(t *testing.T) {
	t.Parallel()
	huge := int64(math.MaxInt64)
	req := protocol.ChatRequest{
		MaxTokens: &huge,
		Messages:  []protocol.ChatMessage{{Role: "user", Content: "hello world"}},
	}
	if got := estimateOf(req, 0); got != math.MaxInt64 {
		t.Fatalf("estimate = %d, want saturation at MaxInt64", got)
	}
	// The tier clamp still applies before the add.
	if got := estimateOf(req, 100); got != 102 {
		t.Fatalf("clamped estimate = %d, want 102", got)
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
	if got := estimateOf(req, 500); got != 503 {
		t.Fatalf("EstimateTokens = %d, want 503 (3 prompt words + 500)", got)
	}
}

// TestCarrierEstimatesScanOnce: the prompt estimate is cached on the
// carrier — one scan per request, by design. The stages downstream of
// ingest read the same request, so a later mutation of the caller's
// chat must not move the cached number.
func TestCarrierEstimatesScanOnce(t *testing.T) {
	t.Parallel()
	maxTokens := int64(10_000)
	chat := protocol.ChatRequest{
		Model:     "mock-model",
		Messages:  []protocol.ChatMessage{{Role: "user", Content: "one two three"}, {Role: "assistant", Content: "four five"}},
		MaxTokens: &maxTokens,
	}
	carrier := &Carrier{Chat: chat}

	if got := carrier.PromptTokens(); got != 5 {
		t.Fatalf("PromptTokens = %d, want the 5 words of both messages", got)
	}

	chat.Messages[0].Content = "completely different text"
	if got := carrier.PromptTokens(); got != 5 {
		t.Fatalf("cached PromptTokens = %d, want the cached 5", got)
	}
	if got, want := carrier.ReserveTokens(500), int64(5+500); got != want {
		t.Fatalf("ReserveTokens = %d, want %d (cached prompt + clamped budget)", got, want)
	}
	if got, want := carrier.EstimatePartialTokens(100), int64(5+25); got != want {
		t.Fatalf("EstimatePartialTokens = %d, want %d (cached prompt + 100/4 bytes)", got, want)
	}
}
