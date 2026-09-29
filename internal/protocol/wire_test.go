/**
 * @file wire_test
 * @description Canonical wire rendering tests: passthrough fidelity and
 * the status clamp guarding against broken upstream status codes.
 */
package protocol

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestChatWireRenderSuccessPassthrough(t *testing.T) {
	t.Parallel()
	header := http.Header{}
	header.Set("Content-Type", "application/json")
	header.Set("Retry-After", "7")
	header.Set("X-Internal", "secret")
	rec := httptest.NewRecorder()

	chatWire{}.RenderSuccess(rec, http.StatusOK, header, []byte(`{"ok":true}`))

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("content type = %q", got)
	}
	if got := rec.Header().Get("Retry-After"); got != "7" {
		t.Fatalf("retry-after = %q", got)
	}
	if got := rec.Header().Get("X-Internal"); got != "" {
		t.Fatalf("internal header leaked: %q", got)
	}
	if got := rec.Body.String(); got != `{"ok":true}` {
		t.Fatalf("body = %q", got)
	}
}

func TestRenderExchangeBodyClampsUnrenderableStatus(t *testing.T) {
	t.Parallel()
	for _, status := range []int{0, 42, 600, 999} {
		rec := httptest.NewRecorder()
		renderExchangeBody(rec, status, http.Header{}, []byte("boom"), PassthroughHeaderNames())
		if rec.Code != http.StatusBadGateway {
			t.Fatalf("status %d rendered as %d, want clamped 502", status, rec.Code)
		}
	}
}

func TestValidHeaderValue(t *testing.T) {
	t.Parallel()
	for _, v := range []string{"application/json", "7", "text/event-stream; charset=utf-8", "a b", "a\tb", "na\xefve"} {
		if !ValidHeaderValue(v) {
			t.Errorf("ValidHeaderValue(%q) = false, want true", v)
		}
	}
	for _, v := range []string{"a\r\nSet-Cookie: x", "a\nb", "a\x00b", "a\x7f", "a\x1bb"} {
		if ValidHeaderValue(v) {
			t.Errorf("ValidHeaderValue(%q) = true, want false", v)
		}
	}
}

// TestRenderExchangeBodyDropsIllegalHeaderValues pins the passthrough's
// upstream-boundary check: a hostile or broken upstream cannot put
// control bytes into the client response's forwarded headers — the
// illegal value is dropped, the legal one still forwarded.
func TestRenderExchangeBodyDropsIllegalHeaderValues(t *testing.T) {
	t.Parallel()
	header := http.Header{}
	header.Set("Content-Type", "application/json")
	header.Set("Retry-After", "3\r\nSet-Cookie: injected=1")

	rec := httptest.NewRecorder()
	renderExchangeBody(rec, http.StatusTooManyRequests, header, []byte("body"), PassthroughHeaderNames())

	if got := rec.Header().Get("Retry-After"); got != "" {
		t.Fatalf("retry-after = %q, want the illegal value dropped", got)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("content type = %q, want the legal value forwarded", got)
	}
}

// TestPassthroughHeaderNamesContract pins the shared passthrough set —
// the one list both the response wires and the cache's replay forward —
// and the copy semantics of the hand-out: a caller mutating its slice
// must not rewrite the set for the whole process.
func TestPassthroughHeaderNamesContract(t *testing.T) {
	t.Parallel()
	got := PassthroughHeaderNames()
	want := []string{"Content-Type", "Retry-After"}
	if len(got) != len(want) {
		t.Fatalf("passthrough set = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("passthrough set = %v, want %v", got, want)
		}
	}

	got[0] = "X-Injected"
	again := PassthroughHeaderNames()
	if again[0] != "Content-Type" {
		t.Fatalf("mutating a returned slice changed the shared set: %v", again)
	}
}
