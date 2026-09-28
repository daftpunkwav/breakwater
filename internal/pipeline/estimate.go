/**
 * @file estimate
 * @description Pre-call token estimation shared by every reservation
 * (TPM bucket and quota lease).
 *
 * Responsibilities:
 * - Produce the single estimate the pipeline passes to both governance
 *   reservations, with the oversized-request clamp applied
 * - Nothing else: post-call correction belongs to the refund stages
 *
 * The estimate is deliberately conservative in the self-protection
 * direction: an unknown completion length reserves the safe default, a
 * declared max_tokens is clamped to the tenant's per-request cap so a
 * short prompt with a huge max_tokens cannot monopolize a tenant bucket
 * (the self-inflicted DoS guard).
 */
package pipeline

import (
	"unicode"

	"github.com/daftpunkwav/breakwater/internal/protocol"
)

// DefaultCompletionReserve is the completion size reserved when the
// request declares no max_tokens. A provisioning default, not a law.
const DefaultCompletionReserve int64 = 256

// EstimateTokens returns the pre-call token estimate of a chat request:
// prompt words plus the clamped completion budget. One token is
// approximated as one whitespace-separated word, matching the mock
// upstream's accounting so evidence runs stay consistent.
//
// maxRequestTokens is the tenant's per-request cap; values below one
// disable the clamp.
func EstimateTokens(req protocol.ChatRequest, maxRequestTokens int64) int64 {
	return promptEstimate(req) + completionEstimate(req, maxRequestTokens)
}

// EstimatePartialTokens estimates usage for a stream that ended
// without a usage report: the full prompt estimate plus the delivered
// bytes converted at the usual rough four bytes per token (the
// local fallback).
func EstimatePartialTokens(req protocol.ChatRequest, streamBytes int64) int64 {
	return promptEstimate(req) + streamBytes/4
}

// PromptTokens returns the prompt-only estimate: the input size the
// context-window pre-filter compares against per-model ceilings. The
// reservation math stays in EstimateTokens, which adds the completion
// budget on top.
func PromptTokens(req protocol.ChatRequest) int64 {
	return promptEstimate(req)
}

// PromptTokens returns the prompt-only estimate, scanning the canonical
// messages at most once per request and caching the result on the
// carrier. The reservation estimate, the context-window pre-filter and
// the partial-stream meter all read the same number of the same
// request, so each re-counting the prompt would repeat an O(prompt)
// scan on the request path. The carrier is single-goroutine by
// contract, so the cache needs no locking.
func (c *Carrier) PromptTokens() int64 {
	if !c.promptKnown {
		c.promptTokens = promptEstimate(c.Chat)
		c.promptKnown = true
	}
	return c.promptTokens
}

// ReserveTokens returns the pre-call token estimate of the request on
// the carrier: the cached prompt estimate plus the clamped completion
// budget. It is the carrier form of EstimateTokens — the reservation
// stages' entry point, so the prompt scan is shared instead of
// repeated.
func (c *Carrier) ReserveTokens(maxRequestTokens int64) int64 {
	return c.PromptTokens() + completionEstimate(c.Chat, maxRequestTokens)
}

// EstimatePartialTokens estimates usage for a stream that ended without
// a usage report, from the carrier's cached prompt estimate. It is the
// carrier form of EstimatePartialTokens.
func (c *Carrier) EstimatePartialTokens(streamBytes int64) int64 {
	return c.PromptTokens() + streamBytes/4
}

func promptEstimate(req protocol.ChatRequest) int64 {
	prompt := int64(0)
	for _, m := range req.Messages {
		prompt += countWords(m.Content)
	}
	if prompt == 0 {
		prompt = 1
	}
	return prompt
}

// countWords counts whitespace-separated words — the same number
// strings.Fields would produce — without materializing the field slice:
// a large prompt would otherwise allocate a large transient slice on
// every request just to be counted.
func countWords(s string) int64 {
	n := int64(0)
	inWord := false
	for _, r := range s {
		if unicode.IsSpace(r) {
			inWord = false
			continue
		}
		if !inWord {
			inWord = true
			n++
		}
	}
	return n
}

func completionEstimate(req protocol.ChatRequest, maxRequestTokens int64) int64 {
	completion := DefaultCompletionReserve
	if req.MaxTokens != nil && *req.MaxTokens > 0 {
		completion = *req.MaxTokens
	}
	if maxRequestTokens >= 1 && completion > maxRequestTokens {
		completion = maxRequestTokens
	}
	return completion
}
