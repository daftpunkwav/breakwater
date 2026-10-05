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

// TestForwardedContentType pins the media-type policy: a type a browser
// would render as a document, sniff into one, or cannot parse at all is
// replaced by the fallback; everything a client can actually parse is
// relayed verbatim; and a value that is not a legal field value never
// reaches the wire. The document cases cover all three families the rule
// names — the HTML and XML MIME types a browser uses as supplied, the
// unknown labels it hands to the sniffing algorithm, and the multipart
// push type whose parts are navigated one by one.
func TestForwardedContentType(t *testing.T) {
	t.Parallel()
	const fallback = "text/plain; charset=utf-8"
	for _, tc := range []struct {
		name string
		in   string
		want string
	}{
		{"json", "application/json", "application/json"},
		{"json with charset", "application/json; charset=utf-8", "application/json; charset=utf-8"},
		{"sse", "text/event-stream", "text/event-stream"},
		{"plain text", "text/plain; charset=utf-8", "text/plain; charset=utf-8"},
		{"octet stream", "application/octet-stream", "application/octet-stream"},
		{"problem json", "application/problem+json", "application/problem+json"},
		{"multipart mixed", "multipart/mixed; boundary=x", "multipart/mixed; boundary=x"},
		{"html", "text/html", fallback},
		{"html with charset", "text/html; charset=utf-8", fallback},
		{"html upper case", "TEXT/HTML", fallback},
		{"xhtml", "application/xhtml+xml", fallback},
		{"svg", "image/svg+xml", fallback},
		{"xml", "application/xml", fallback},
		{"text xml", "text/xml", fallback},
		{"other xml family", "application/rss+xml", fallback},
		{"application unknown", "application/unknown", fallback},
		{"unknown unknown", "unknown/unknown", fallback},
		{"wildcard", "*/*", fallback},
		{"unknown label with parameters", "application/unknown; charset=utf-8", fallback},
		{"multipart mixed replace", "multipart/x-mixed-replace; boundary=x", fallback},
		{"json is not the xml family", "application/vnd.api+json", "application/vnd.api+json"},
		{"empty", "", fallback},
		{"illegal", "text/html\r\nSet-Cookie: x", fallback},
		{"crlf inside a quoted parameter", "text/plain; x=\"a\r\nb\"", fallback},
		{"unterminated quoted parameter", "text/plain; x=\"a", fallback},
		{"parameter without a value", "text/plain; charset", fallback},
		{"bare token, no subtype", "not-a-media-type", fallback},
		{"bare wildcard, no subtype", "*", fallback},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := ForwardedContentType(tc.in, fallback); got != tc.want {
				t.Fatalf("ForwardedContentType(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestRenderExchangeBodyReplacesDocumentContentType pins the passthrough
// boundary: the upstream's bytes are relayed unchanged, but a media type
// a browser would render as a document is not, and a body the upstream
// left unlabelled is not left to net/http's sniffing either.
func TestRenderExchangeBodyReplacesDocumentContentType(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		ct   string
		want string
	}{
		{"document", "text/html", PlainContentType},
		{"unlabelled", "", PlainContentType},
		{"parseable", "application/json", "application/json"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			header := http.Header{}
			if tc.ct != "" {
				header.Set("Content-Type", tc.ct)
			}
			rec := httptest.NewRecorder()
			renderExchangeBody(rec, http.StatusBadGateway, header, []byte("<script>alert(1)</script>"), PassthroughHeaderNames())

			if got := rec.Header().Get("Content-Type"); got != tc.want {
				t.Fatalf("content type = %q, want %q", got, tc.want)
			}
			if got := rec.Body.String(); got != "<script>alert(1)</script>" {
				t.Fatalf("body = %q, want the upstream bytes unchanged", got)
			}
		})
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
