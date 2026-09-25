/**
 * @file error_envelope_test
 * @description The gateway-originated error envelopes of every wire and
 * the extraction of upstream error payloads into them.
 */
package protocol

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestWriteErrorEnvelope(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	WriteError(rec, http.StatusTooManyRequests, string(CodeRateLimited), "slow down")

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("status = %d, want 429", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != "application/json" {
		t.Fatalf("content type = %q", got)
	}
	body := rec.Body.String()
	for _, want := range []string{
		`"message":"slow down"`,
		`"type":"` + ErrorType + `"`,
		`"code":"rate_limit_exceeded"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("body %s missing %s", body, want)
		}
	}
}

func TestChatWireRenderErrorUsesOpenAIEnvelope(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	chatWire{}.RenderError(rec, http.StatusServiceUnavailable, "governance_unavailable", "rate limiter unavailable")

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"type":"gateway_error"`) || !strings.Contains(body, `"code":"governance_unavailable"`) {
		t.Fatalf("body = %s", body)
	}
}

func TestParseUpstreamErrorFallbacks(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		body    []byte
		message string
		code    string
	}{
		{"complete envelope", []byte(`{"error":{"message":"ctx too long","code":"context_length"}}`), "ctx too long", "context_length"},
		{"message without code", []byte(`{"error":{"message":"boom"}}`), "boom", "upstream_error"},
		{"empty object", []byte(`{}`), "upstream request failed", "upstream_error"},
		{"garbage body", []byte(`<html>502</html>`), "upstream request failed", "upstream_error"},
	}
	for _, tc := range cases {
		message, code := parseUpstreamError(tc.body)
		if message != tc.message || code != tc.code {
			t.Errorf("%s: message = %q code = %q, want %q/%q", tc.name, message, code, tc.message, tc.code)
		}
	}
}

func TestAnthropicWireRenderUpstreamError(t *testing.T) {
	t.Parallel()
	upstream := []byte(`{"error":{"message":"the model is overloaded","code":"overloaded"}}`)
	rec := httptest.NewRecorder()
	anthropicWire{}.RenderUpstreamError(rec, http.StatusBadGateway, headerOf("application/json"), upstream)

	if rec.Code != http.StatusBadGateway {
		t.Fatalf("status = %d, want the upstream status kept", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, `"type":"error"`) || !strings.Contains(body, `"message":"the model is overloaded"`) {
		t.Fatalf("body = %s", body)
	}
}

func TestResponsesWireRenderUpstreamErrorPassthrough(t *testing.T) {
	t.Parallel()
	upstream := []byte(`{"error":{"message":"overloaded","code":"5xx"}}`)
	header := headerOf("application/json")
	header.Set("Retry-After", "3")
	rec := httptest.NewRecorder()
	responsesWire{}.RenderUpstreamError(rec, http.StatusInternalServerError, header, upstream)

	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d", rec.Code)
	}
	if got := rec.Header().Get("Retry-After"); got != "3" {
		t.Fatalf("retry-after = %q, want passthrough", got)
	}
	if got := rec.Body.String(); got != string(upstream) {
		t.Fatalf("body = %q, want verbatim %q", got, upstream)
	}
}

func TestResponsesWireRenderError(t *testing.T) {
	t.Parallel()
	rec := httptest.NewRecorder()
	responsesWire{}.RenderError(rec, http.StatusPaymentRequired, string(CodeInsufficientQuota), "drained")

	if rec.Code != http.StatusPaymentRequired {
		t.Fatalf("status = %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"code":"insufficient_quota"`) {
		t.Fatalf("body = %s", rec.Body.String())
	}
}

func TestAnthropicRenderErrorTypeMappings(t *testing.T) {
	t.Parallel()
	cases := []struct {
		code    string
		errType string
	}{
		{string(CodeInsufficientQuota), "billing_error"},
		{"governance_unavailable", "api_error"}, // unmapped codes keep the generic type
	}
	for _, tc := range cases {
		rec := httptest.NewRecorder()
		anthropicWire{}.RenderError(rec, http.StatusForbidden, tc.code, "no")
		if !strings.Contains(rec.Body.String(), `"type":"`+tc.errType+`"`) {
			t.Errorf("code %s rendered as %s, want %s:\n%s", tc.code, tc.errType, tc.errType, rec.Body.String())
		}
	}
}
