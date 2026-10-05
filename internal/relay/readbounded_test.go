/**
 * @file readbounded_test
 * @description The bounded body read's size hint is a performance hint
 * only: a lying or missing Content-Length must read the same bytes and
 * enforce the same limit as an exact one.
 */
package relay

import (
	"math"
	"strings"
	"testing"
)

func TestReadBoundedHintIsHintOnly(t *testing.T) {
	body := strings.NewReader("hello upstream")
	got, err := readBounded(body, 1<<10, int64(len("hello upstream")))
	if err != nil || string(got) != "hello upstream" {
		t.Fatalf("exact hint: got %q err %v", got, err)
	}

	// A hint smaller than the body must not truncate the read.
	body = strings.NewReader("hello upstream")
	got, err = readBounded(body, 1<<10, 4)
	if err != nil || string(got) != "hello upstream" {
		t.Fatalf("short hint: got %q err %v", got, err)
	}

	// A hint larger than the body must not pad it.
	body = strings.NewReader("hello upstream")
	got, err = readBounded(body, 1<<10, 4096)
	if err != nil || string(got) != "hello upstream" {
		t.Fatalf("long hint: got %q err %v", got, err)
	}

	// No hint at all reads the same bytes.
	body = strings.NewReader("hello upstream")
	got, err = readBounded(body, 1<<10, -1)
	if err != nil || string(got) != "hello upstream" {
		t.Fatalf("no hint: got %q err %v", got, err)
	}
}

func TestReadBoundedStillFailsClosedPastLimit(t *testing.T) {
	body := strings.NewReader(strings.Repeat("x", 100))
	if _, err := readBounded(body, 50, -1); err == nil {
		t.Fatal("oversized body must fail closed")
	}
	// The limit governs the hint too: a hint past the limit must not
	// become the buffer size.
	body = strings.NewReader(strings.Repeat("x", 100))
	if _, err := readBounded(body, 50, 1<<20); err == nil {
		t.Fatal("oversized body with oversized hint must fail closed")
	}
}

func TestContentLengthHint(t *testing.T) {
	cases := []struct {
		name   string
		header map[string][]string
		want   int64
	}{
		{"absent", map[string][]string{}, -1},
		{"nil header", nil, -1},
		{"present", map[string][]string{"Content-Length": {"123"}}, 123},
		{"garbage", map[string][]string{"Content-Length": {"many"}}, -1},
		{"negative", map[string][]string{"Content-Length": {"-5"}}, -1},
	}
	for _, tc := range cases {
		if got := contentLengthHint(tc.header); got != tc.want {
			t.Errorf("%s: hint = %d, want %d", tc.name, got, tc.want)
		}
	}
}

func TestGrowSeedCapsBeforeNarrowing(t *testing.T) {
	cases := []struct {
		name     string
		sizeHint int64
		want     int
	}{
		{"small hint keeps headroom", 4, 4 + readSeedHeadroom},
		{"largest in-range hint", math.MaxInt32 - readSeedHeadroom, math.MaxInt32},
		{"first hint past the cap", math.MaxInt32 - readSeedHeadroom + 1, 0},
		{"hint past the cap skips pre-sizing", math.MaxInt64, 0},
	}
	for _, tc := range cases {
		if got := growSeed(tc.sizeHint); got != tc.want {
			t.Errorf("%s: growSeed(%d) = %d, want %d", tc.name, tc.sizeHint, got, tc.want)
		}
	}
}
