/**
 * @file tokens
 * @description The shared measure of a message's token cost.
 *
 * Responsibilities:
 * - Estimate how many tokens one message content costs
 * - Nothing else: the reservation policy (the clamped completion
 *   budget, the tenant's per-request cap) belongs to the pipeline, and
 *   the settlement belongs to the quota ledger
 *
 * The measure has one home because two parties must agree on it: the
 * gateway reserves against it, and the upstream reports what it
 * charged. A settlement refunds a reservation down to the reported
 * usage, so a mock upstream that counted differently would refund a
 * tenant down to a number it never spent.
 *
 * One token is one whitespace-separated word — a word is about four
 * bytes — and a content with no whitespace at all (CJK prose, base64,
 * minified code) is measured by its bytes instead. The two rules agree
 * on ordinary prose, so the byte rule only speaks where the word rule
 * is blind: a kilobyte of whitespace-free content is hundreds of
 * tokens, not one.
 */
package protocol

import "unicode"

// BytesPerToken is the rough byte size of one token — the rule the
// byte-side measurements share, so the prompt measure and the
// stream-byte measure cannot drift apart.
const BytesPerToken = 4

// EstimateContentTokens estimates the tokens of one message content: the
// whitespace-separated word count, floored by the byte size of the
// content.
//
// The floor is what keeps a whitespace-free content from pricing as a
// single token. The word rule alone cannot see it: CJK prose, base64
// and minified code carry no spaces, so every one of them counts as
// one word however large it is — and a prompt-sized spend that prices
// as one token escapes the very controls that exist to bound it.
func EstimateContentTokens(content string) int64 {
	return max(countWords(content), int64(len(content))/BytesPerToken)
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
