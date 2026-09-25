/**
 * @file faults_test
 * @description The X-Mockllm-* directive parser: full valid sets,
 * defaults, the slow-mode chunk-delay default, and every strict
 * validation failure.
 */
package mockllm

import (
	"net/http"
	"testing"
	"time"
)

func TestParseFaults(t *testing.T) {
	t.Parallel()

	t.Run("defaults when no headers present", func(t *testing.T) {
		t.Parallel()
		f, err := parseFaults(http.Header{})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if f.StreamMode != StreamModeNormal {
			t.Errorf("stream mode = %q, want %q", f.StreamMode, StreamModeNormal)
		}
		if f.Delay != 0 || f.Status != 0 || f.CompletionTokens != 0 {
			t.Errorf("expected zero-valued directives, got %+v", f)
		}
	})

	t.Run("full valid set", func(t *testing.T) {
		t.Parallel()
		h := http.Header{}
		h.Set(headerDelay, "250")
		h.Set(headerStatus, "503")
		h.Set(headerStreamMode, "slow")
		h.Set(headerChunkDelay, "40")
		h.Set(headerOmitUsage, "true")
		h.Set(headerCompletionTok, "8")
		f, err := parseFaults(h)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if f.Delay != 250*time.Millisecond {
			t.Errorf("delay = %v, want 250ms", f.Delay)
		}
		if f.Status != 503 {
			t.Errorf("status = %d, want 503", f.Status)
		}
		if f.StreamMode != StreamModeSlow {
			t.Errorf("stream mode = %q, want %q", f.StreamMode, StreamModeSlow)
		}
		if f.ChunkDelay != 40*time.Millisecond {
			t.Errorf("chunk delay = %v, want 40ms", f.ChunkDelay)
		}
		if !f.OmitUsage {
			t.Error("omit usage = false, want true")
		}
		if f.CompletionTokens != 8 {
			t.Errorf("completion tokens = %d, want 8", f.CompletionTokens)
		}
	})

	t.Run("slow mode gets default chunk delay", func(t *testing.T) {
		t.Parallel()
		h := http.Header{}
		h.Set(headerStreamMode, "slow")
		f, err := parseFaults(h)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if f.ChunkDelay != defaultChunkDelay {
			t.Errorf("chunk delay = %v, want %v", f.ChunkDelay, defaultChunkDelay)
		}
	})

	t.Run("rejects unknown stream mode", func(t *testing.T) {
		t.Parallel()
		h := http.Header{}
		h.Set(headerStreamMode, "explode")
		if _, err := parseFaults(h); err == nil {
			t.Fatal("expected error for unknown stream mode")
		}
	})

	t.Run("rejects non-error status", func(t *testing.T) {
		t.Parallel()
		h := http.Header{}
		h.Set(headerStatus, "200")
		if _, err := parseFaults(h); err == nil {
			t.Fatal("expected error for non-error status")
		}
	})

	t.Run("rejects negative delay", func(t *testing.T) {
		t.Parallel()
		h := http.Header{}
		h.Set(headerDelay, "-5")
		if _, err := parseFaults(h); err == nil {
			t.Fatal("expected error for negative delay")
		}
	})

	t.Run("rejects oversized completion", func(t *testing.T) {
		t.Parallel()
		h := http.Header{}
		h.Set(headerCompletionTok, "99999999")
		if _, err := parseFaults(h); err == nil {
			t.Fatal("expected error for oversized completion")
		}
	})

	t.Run("rejects malformed delay number", func(t *testing.T) {
		t.Parallel()
		h := http.Header{}
		h.Set(headerDelay, "abc")
		if _, err := parseFaults(h); err == nil {
			t.Fatal("expected error for a malformed delay")
		}
	})

	t.Run("rejects malformed status number", func(t *testing.T) {
		t.Parallel()
		h := http.Header{}
		h.Set(headerStatus, "abc")
		if _, err := parseFaults(h); err == nil {
			t.Fatal("expected error for a malformed status")
		}
	})

	t.Run("rejects malformed chunk delay number", func(t *testing.T) {
		t.Parallel()
		h := http.Header{}
		h.Set(headerChunkDelay, "abc")
		if _, err := parseFaults(h); err == nil {
			t.Fatal("expected error for a malformed chunk delay")
		}
	})

	t.Run("rejects malformed boolean", func(t *testing.T) {
		t.Parallel()
		h := http.Header{}
		h.Set(headerOmitUsage, "maybe")
		if _, err := parseFaults(h); err == nil {
			t.Fatal("expected error for a malformed boolean")
		}
	})

	t.Run("rejects malformed completion tokens", func(t *testing.T) {
		t.Parallel()
		h := http.Header{}
		h.Set(headerCompletionTok, "abc")
		if _, err := parseFaults(h); err == nil {
			t.Fatal("expected error for malformed completion tokens")
		}
	})
}
