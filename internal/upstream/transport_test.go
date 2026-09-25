/**
 * @file transport_test
 * @description The shared upstream transport: the load-tuned
 * connection parameters and the independence from net/http defaults.
 */
package upstream

import (
	"net/http"
	"testing"
	"time"
)

// TestDefaultTransportTuning: the forwarding gateway's transport
// carries the tuned pool parameters, not the net/http defaults.
func TestDefaultTransportTuning(t *testing.T) {
	t.Parallel()

	transport := DefaultTransport()
	if transport.MaxIdleConnsPerHost != defaultMaxIdleConnsPerHost {
		t.Fatalf("MaxIdleConnsPerHost = %d, want %d", transport.MaxIdleConnsPerHost, defaultMaxIdleConnsPerHost)
	}
	if transport.MaxIdleConns != defaultMaxIdleConns {
		t.Fatalf("MaxIdleConns = %d, want %d", transport.MaxIdleConns, defaultMaxIdleConns)
	}
	if transport.MaxConnsPerHost != defaultMaxConnsPerHost {
		t.Fatalf("MaxConnsPerHost = %d, want %d (no per-host cap)", transport.MaxConnsPerHost, defaultMaxConnsPerHost)
	}
	if transport.IdleConnTimeout != defaultIdleConnTimeout {
		t.Fatalf("IdleConnTimeout = %s, want %s", transport.IdleConnTimeout, defaultIdleConnTimeout)
	}
	if !transport.ForceAttemptHTTP2 {
		t.Fatal("ForceAttemptHTTP2 = false, want HTTP/2 attempted")
	}
}

// TestDefaultTransportIsAClone: mutating the returned transport never
// leaks into the process-wide default.
func TestDefaultTransportIsAClone(t *testing.T) {
	t.Parallel()

	before := http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost
	transport := DefaultTransport()
	transport.MaxIdleConnsPerHost = before + 1

	if got := http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost; got != before {
		t.Fatalf("http.DefaultTransport mutated: MaxIdleConnsPerHost = %d, want %d", got, before)
	}
}

// TestDefaultTransportValuesAreProvisioningSane: the tuning constants
// keep their relationship — the per-host pool is bounded by the total
// pool.
func TestDefaultTransportValuesAreProvisioningSane(t *testing.T) {
	t.Parallel()

	if defaultMaxIdleConnsPerHost > defaultMaxIdleConns {
		t.Fatalf("per-host idle pool %d exceeds the total %d",
			defaultMaxIdleConnsPerHost, defaultMaxIdleConns)
	}
	if defaultIdleConnTimeout != 90*time.Second {
		t.Fatalf("IdleConnTimeout = %s, want the tuned 90s", defaultIdleConnTimeout)
	}
}
