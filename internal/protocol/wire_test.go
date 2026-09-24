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
		renderExchangeBody(rec, status, http.Header{}, []byte("boom"), passthroughHeaderNames)
		if rec.Code != http.StatusBadGateway {
			t.Fatalf("status %d rendered as %d, want clamped 502", status, rec.Code)
		}
	}
}
