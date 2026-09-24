/**
 * @file tee_test
 * @description Tee mechanics: capture fidelity, mode behavior, single
 * WriteHeader, flush recording.
 */
package httpserver

import (
	"net/http/httptest"
	"testing"
)

func TestBufferingTeeCapturesAndForwards(t *testing.T) {
	t.Parallel()
	inner := httptest.NewRecorder()
	tee := NewBufferingTee(inner, 64)

	tee.Header().Set("Content-Type", "text/plain")
	tee.WriteHeader(201)
	if _, err := tee.Write([]byte("hello world")); err != nil {
		t.Fatalf("write: %v", err)
	}

	if tee.Status() != 201 || inner.Code != 201 {
		t.Fatalf("status = %d/%d, want 201", tee.Status(), inner.Code)
	}
	if string(tee.Body()) != "hello world" || inner.Body.String() != "hello world" {
		t.Fatalf("capture mismatch: tee=%q inner=%q", tee.Body(), inner.Body.String())
	}
	if tee.Truncated() {
		t.Error("truncated reported within cap")
	}
}

func TestBufferingTeeDropsPastCapButKeepsForwarding(t *testing.T) {
	t.Parallel()
	inner := httptest.NewRecorder()
	tee := NewBufferingTee(inner, 4)

	if _, err := tee.Write([]byte("abcdefgh")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if string(tee.Body()) != "abcd" {
		t.Fatalf("retained = %q, want truncated prefix abcd", tee.Body())
	}
	if !tee.Truncated() {
		t.Error("truncation not reported")
	}
	if inner.Body.String() != "abcdefgh" {
		t.Fatalf("forwarded = %q, want full body", inner.Body.String())
	}
	if got := tee.BytesWritten(); got != 8 {
		t.Fatalf("bytes written = %d, want 8", got)
	}
}

func TestCountingTeeNeverRetains(t *testing.T) {
	t.Parallel()
	inner := httptest.NewRecorder()
	tee := NewCountingTee(inner)

	if _, err := tee.Write([]byte("stream chunk")); err != nil {
		t.Fatalf("write: %v", err)
	}
	if len(tee.Body()) != 0 {
		t.Fatalf("counting tee retained %q", tee.Body())
	}
	if got := tee.BytesWritten(); got != 12 {
		t.Fatalf("bytes written = %d, want 12", got)
	}
}

func TestTeeWriteHeaderOnce(t *testing.T) {
	t.Parallel()
	inner := httptest.NewRecorder()
	tee := NewBufferingTee(inner, 16)

	tee.WriteHeader(200)
	tee.WriteHeader(500) // must be absorbed

	if inner.Code != 200 {
		t.Fatalf("code = %d, want the first status 200", inner.Code)
	}
}

func TestTeeRecordsFlush(t *testing.T) {
	t.Parallel()
	inner := httptest.NewRecorder()
	tee := NewCountingTee(inner)

	if tee.Flushed() {
		t.Fatal("flushed before any flush")
	}
	tee.Flush()
	if !tee.Flushed() {
		t.Fatal("flush not recorded")
	}
}
