/**
 * @file credential_rotation_test
 * @description Credential-aware failover inside one upstream: a
 * credential-class failure hands the request to the next ring
 * credential instead of ending it, the burned credential is excluded
 * on the way, a rate-limited credential's own hint does not delay the
 * hand-off, and a fully retired ring restores the terminal verdict.
 */
package relay

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/daftpunkwav/breakwater/internal/retry"
	"github.com/daftpunkwav/breakwater/internal/upstream"
)

// keyFailure is one credential's scripted failure; an unlisted
// credential passes with a 200.
type keyFailure struct {
	status int
	body   string
	hint   string
}

// keyServer answers per bearer credential, recording every
// Authorization header it saw.
type keyServer struct {
	server *httptest.Server
	mu     sync.Mutex
	saw    []string
}

func newKeyServer(t *testing.T, fail map[string]keyFailure) *keyServer {
	t.Helper()
	ks := &keyServer{}
	ks.server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		ks.mu.Lock()
		ks.saw = append(ks.saw, key)
		ks.mu.Unlock()
		if f, ok := fail[key]; ok {
			if f.hint != "" {
				w.Header().Set("Retry-After", f.hint)
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(f.status)
			// nosemgrep: go.lang.security.audit.xss.no-io-writestring-to-responsewriter.no-io-writestring-to-responsewriter -- test fixture writes an opaque response body; no HTML is rendered
			_, _ = io.WriteString(w, f.body)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	t.Cleanup(ks.server.Close)
	return ks
}

func (ks *keyServer) requests() []string {
	ks.mu.Lock()
	defer ks.mu.Unlock()
	return append([]string(nil), ks.saw...)
}

func newRingUpstream(t *testing.T, base string, keys ...string) *upstream.OpenAI {
	t.Helper()
	adapter, err := upstream.NewOpenAI(upstream.OpenAIConfig{ID: "ring", BaseURL: base, APIKeys: keys})
	if err != nil {
		t.Fatalf("adapter: %v", err)
	}
	return adapter
}

// retireHook returns a fatal hook shaped like the assembly's: the
// report retires the credential in the adapter's ring.
func retireHook(adapter *upstream.OpenAI, reports *[]string) func(string, int, string) {
	return func(id string, cred int, reason string) {
		adapter.RetireCredential(cred)
		*reports = append(*reports, id+"/"+reason)
	}
}

// TestCredentialFailureRotatesWithinTheRequest: the first credential
// is fatally rejected, the hook retires it, and the next attempt —
// with the burned credential excluded — succeeds on the second one.
func TestCredentialFailureRotatesWithinTheRequest(t *testing.T) {
	t.Parallel()
	ks, adapter := newUnauthorizedRing(t)

	var reports []string
	exec := New(testPolicy(), nil, WithUpstreamFatalHook(retireHook(adapter, &reports)))
	result := execute(t, exec, []upstream.Upstream{adapter}, false, `{}`)
	if result.Status != http.StatusOK {
		t.Fatalf("status = %d body = %s, want success on the second credential", result.Status, result.Body)
	}
	if len(reports) != 1 || reports[0] != "ring/upstream_auth_failure" {
		t.Fatalf("reports = %v, want exactly one auth-failure report", reports)
	}
	saw := ks.requests()
	if len(saw) != 2 || saw[0] != "sk-bad" || saw[1] != "sk-good" {
		t.Fatalf("credentials seen = %v, want the walk from the dead to the live one", saw)
	}
}

// TestCredentialRateLimitRotatesWithoutTheHint: a 429 on the first
// credential excludes it and the exchange moves to the next one — the
// failed credential's Retry-After must not delay that hand-off.
func TestCredentialRateLimitRotatesWithoutTheHint(t *testing.T) {
	t.Parallel()
	ks := newKeyServer(t, map[string]keyFailure{
		"sk-busy": {status: http.StatusTooManyRequests,
			body: `{"error":{"type":"rate_limit_error"}}`, hint: "30"},
	})
	adapter := newRingUpstream(t, ks.server.URL, "sk-busy", "sk-free")

	exec := New(retry.Policy{MaxAttempts: 2}, nil)
	start := time.Now()
	result := execute(t, exec, []upstream.Upstream{adapter}, false, `{}`)
	if result.Status != http.StatusOK {
		t.Fatalf("status = %d body = %s, want success on the second credential", result.Status, result.Body)
	}
	if elapsed := time.Since(start); elapsed > 10*time.Second {
		t.Fatalf("rotation took %v; the burned credential's 30s hint must not delay it", elapsed)
	}
	saw := ks.requests()
	if len(saw) != 2 || saw[0] != "sk-busy" || saw[1] != "sk-free" {
		t.Fatalf("credentials seen = %v, want the walk off the rate-limited one", saw)
	}
}

// TestFullyRetiredRingRestoresTerminalVerdict: when the walk retires
// the last living credential, the final 401 is terminal again — no
// third attempt, the passthrough the client would have gotten from a
// ringless upstream.
func TestFullyRetiredRingRestoresTerminalVerdict(t *testing.T) {
	t.Parallel()
	ks := newKeyServer(t, map[string]keyFailure{
		"sk-dead":  {status: http.StatusUnauthorized, body: `{"error":{"type":"authentication_error"}}`},
		"sk-older": {status: http.StatusUnauthorized, body: `{"error":{"type":"authentication_error"}}`},
	})
	adapter := newRingUpstream(t, ks.server.URL, "sk-dead", "sk-older")

	var reports []string
	exec := New(testPolicy(), nil, WithUpstreamFatalHook(retireHook(adapter, &reports)))
	result := execute(t, exec, []upstream.Upstream{adapter}, false, `{}`)
	if result.Status != http.StatusUnauthorized {
		t.Fatalf("status = %d body = %s, want the terminal 401 passthrough", result.Status, result.Body)
	}
	if got := adapter.AliveCredentials(); got != 0 {
		t.Fatalf("alive credentials = %d, want 0", got)
	}
	saw := ks.requests()
	if len(saw) != 2 {
		t.Fatalf("exchanges = %d, want one per credential and no third attempt", len(saw))
	}
}
