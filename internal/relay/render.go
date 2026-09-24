/**
 * @file render
 * @description Client-side response rendering for the relay: verbatim
 * passthrough of upstream replies and gateway error envelopes.
 *
 * Responsibilities:
 * - Write one captured upstream exchange to the client, copying the
 *   headers a client may act on (content type, retry timing)
 * - Render the gateway's own failures in the OpenAI error envelope so
 *   SDKs surface them as first-class errors
 * - Nothing else: SSE event bytes are rendered by the pump and the
 *   protocol codec; this file owns whole-body HTTP responses
 */
package relay

import (
	"net/http"

	"github.com/daftpunkwav/breakwater/internal/protocol"
)

// passthroughHeaders are the upstream response headers a client may act
// on and are therefore forwarded; everything else is hop-by-hop or
// gateway-owned.
var passthroughHeaders = []string{
	"Content-Type",
	"Retry-After",
}

// renderExchange writes one captured upstream exchange verbatim.
func renderExchange(out http.ResponseWriter, snap *exchangeSnapshot) {
	header := out.Header()
	for _, name := range passthroughHeaders {
		if v := snap.header.Get(name); v != "" {
			header.Set(name, v)
		}
	}
	out.WriteHeader(snap.status)
	_, _ = out.Write(snap.body)
}

// renderGatewayError renders a gateway-originated failure in the
// OpenAI error envelope; providers' own errors are passed through by
// renderExchange instead.
func renderGatewayError(out http.ResponseWriter, status int, code, message string) {
	protocol.WriteError(out, status, code, message)
}
