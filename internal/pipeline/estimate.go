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
 * (the self-inflicted DoS guard), and the prompt itself is measured by
 * protocol.EstimateContentTokens — the measure the upstream's reported
 * usage is held to, so a reservation can never price below what the
 * tenant really spends.
 */
package pipeline

import (
	"math"

	"github.com/daftpunkwav/breakwater/internal/protocol"
)

// DefaultCompletionReserve is the completion size reserved when the
// request declares no max_tokens. A provisioning default, not a law.
const DefaultCompletionReserve int64 = 256

// EstimateTokens returns the pre-call token estimate of a chat request:
// prompt tokens plus the clamped completion budget. One token is
// approximated as one whitespace-separated word, floored by the
// four-bytes-per-token rule, matching the mock upstream's accounting so
// evidence runs stay consistent.
//
// maxRequestTokens is the tenant's per-request cap; values below one
// disable the clamp.
func EstimateTokens(req protocol.ChatRequest, maxRequestTokens int64) int64 {
	return addSaturating(promptEstimate(req), completionEstimate(req, maxRequestTokens))
}

// EstimatePartialTokens estimates usage for a stream that ended
// without a usage report: the full prompt estimate plus the delivered
// bytes converted at the usual rough four bytes per token (the
// local fallback).
func EstimatePartialTokens(req protocol.ChatRequest, streamBytes int64) int64 {
	return promptEstimate(req) + streamBytes/protocol.BytesPerToken
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
	return addSaturating(c.PromptTokens(), completionEstimate(c.Chat, maxRequestTokens))
}

// EstimatePartialTokens estimates usage for a stream that ended without
// a usage report, from the carrier's cached prompt estimate. It is the
// carrier form of EstimatePartialTokens.
func (c *Carrier) EstimatePartialTokens(streamBytes int64) int64 {
	return c.PromptTokens() + streamBytes/protocol.BytesPerToken
}

func promptEstimate(req protocol.ChatRequest) int64 {
	prompt := int64(0)
	for _, m := range req.Messages {
		prompt += protocol.EstimateContentTokens(m.Content)
	}
	if prompt == 0 {
		prompt = 1
	}
	return prompt
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

// addSaturating returns a+b clamped to math.MaxInt64 instead of
// wrapping. Both operands are non-negative estimates; a wrapped sum
// would go negative, and a negative reservation reads to the limiter
// as affordable and to the ledger as a balance credit — free money
// minted by an overflow. A client-declared max_tokens near the int64
// ceiling against a tier without its own clamp reaches exactly that.
func addSaturating(a, b int64) int64 {
	sum := a + b
	if sum < 0 {
		return math.MaxInt64
	}
	return sum
}
