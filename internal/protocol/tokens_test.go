/**
 * @file tokens_test
 * @description The shared content measure: word counting matches
 * strings.Fields exactly, and a content without whitespace is measured
 * by its bytes instead of pricing as a single token.
 */
package protocol

import (
	"strings"
	"testing"
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

// TestEstimateContentTokensFloorsWhitespaceFreeContent: a content
// without whitespace carries no word for the word rule to count, so
// the byte rule has to speak for it. Otherwise a kilobyte of CJK prose
// or base64 prices as a single token, and the controls that bound a
// tenant's token spend never see it.
func TestEstimateContentTokensFloorsWhitespaceFreeContent(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		content string
	}{
		{"CJK prose", strings.Repeat("中文字幕", 100)},
		{"base64 blob", strings.Repeat("QUJDREVGR0hJSktMTU5PUFFSU1RVVldYWVowMTIzNDU2Nzg5", 20)},
		{"minified code", strings.Repeat("a(b,c)", 100)},
	}
	for _, tc := range cases {
		got := EstimateContentTokens(tc.content)
		want := int64(len(tc.content)) / BytesPerToken
		if got != want {
			t.Errorf("%s: estimate = %d, want the byte floor %d", tc.name, got, want)
		}
		if got <= 1 {
			t.Errorf("%s: estimate = %d, want more than a single token", tc.name, got)
		}
	}
}

// TestEstimateContentTokensKeepsProseCloseToItsWordCount: ordinary
// prose is measured by its words, with the byte floor agreeing to
// within a token — the measure must not drift far above what the
// content actually costs.
func TestEstimateContentTokensKeepsProseCloseToItsWordCount(t *testing.T) {
	t.Parallel()
	const prose = "the quick brown fox jumps over the lazy dog"
	words := int64(len(strings.Fields(prose)))
	got := EstimateContentTokens(prose)
	if got < words {
		t.Fatalf("estimate = %d, want at least the %d words", got, words)
	}
	if got > words+1 {
		t.Fatalf("estimate = %d, want at most one token above the %d words", got, words)
	}
	if got := EstimateContentTokens(""); got != 0 {
		t.Fatalf("empty estimate = %d, want 0", got)
	}
}
