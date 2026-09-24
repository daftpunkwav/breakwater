/**
 * @file transport
 * @description Tuned HTTP transport for upstream exchanges.
 *
 * Responsibilities:
 * - Provide one shared, explicitly tuned *http.Transport
 *
 * The net/http defaults are wrong for a forwarding gateway:
 * MaxIdleConnsPerHost defaults to 2, which under concurrency tears down
 * and rebuilds connections in a storm of TIME_WAIT sockets. The values
 * here are provisioning defaults, tuned by load test evidence, not
 * constants of nature.
 */
package upstream

import (
	"net/http"
	"time"
)

// Tuned transport values. Idle connections per host dominate forwarding
// latency under load; the total cap bounds how far the gateway may
// reach into one provider.
const (
	defaultMaxIdleConnsPerHost = 100
	defaultMaxConnsPerHost     = 0 // no per-host hard cap by default
	defaultMaxIdleConns        = 1000
	defaultIdleConnTimeout     = 90 * time.Second
)

// DefaultTransport builds the transport every adapter shares unless a
// caller injects its own. HTTP/2 is attempted where available.
func DefaultTransport() *http.Transport {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConnsPerHost = defaultMaxIdleConnsPerHost
	transport.MaxIdleConns = defaultMaxIdleConns
	transport.MaxConnsPerHost = defaultMaxConnsPerHost
	transport.IdleConnTimeout = defaultIdleConnTimeout
	transport.ForceAttemptHTTP2 = true
	return transport
}
