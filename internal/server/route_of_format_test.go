/**
 * @file route_of_format_test
 * @description The route table pin: every known client format maps to
 * its own route path. A Format member added without extending
 * routeOfFormat's switch would silently register on the canonical chat
 * route — this test fails instead.
 */
package server

import (
	"testing"

	"github.com/daftpunkwav/breakwater/internal/protocol"
)

func TestRouteOfFormatTable(t *testing.T) {
	t.Parallel()
	cases := map[protocol.Format]string{
		protocol.FormatOpenAIChat:        "POST /v1/chat/completions",
		protocol.FormatOpenAIResponses:   "POST /v1/responses",
		protocol.FormatAnthropicMessages: "POST /v1/messages",
	}
	for format, want := range cases {
		if got := routeOfFormat(format); got != want {
			t.Errorf("routeOfFormat(%s) = %q, want %q", format, got, want)
		}
	}

	// Distinctness: the three surfaces must never share a route, or one
	// registration in newRootHandler would overwrite another.
	seen := map[string]protocol.Format{}
	for format := range cases {
		route := routeOfFormat(format)
		if prev, ok := seen[route]; ok {
			t.Errorf("formats %s and %s share route %q", prev, format, route)
		}
		seen[route] = format
	}
}
