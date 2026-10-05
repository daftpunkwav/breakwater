/**
 * @file access_trail_test
 * @description The access trail: one trace per upstream attempt in try
 * order — the failover walk a request took, including the credentials
 * each exchange used and the status each completed with.
 */
package relay

import (
	"context"
	"net/http"
	"testing"

	"github.com/daftpunkwav/breakwater/internal/circuit"
	"github.com/daftpunkwav/breakwater/internal/upstream"
)

// TestTrailRecordsTheFailoverWalk: a request that fails over from a
// rate-limited candidate to a healthy one carries both exchanges in
// order, with their statuses.
func TestTrailRecordsTheFailoverWalk(t *testing.T) {
	t.Parallel()
	rateLimited := jsonStubUpstream(t, "slow", http.StatusTooManyRequests, `{"error":{"type":"rate_limit_error"}}`)
	healthy := jsonStubUpstream(t, "fast", http.StatusOK, `{"ok":true}`)
	exec := New(testPolicy(), nil)
	result := execute(t, exec, []upstream.Upstream{rateLimited, healthy}, false, `{}`)
	if result.Status != http.StatusOK {
		t.Fatalf("status = %d, want success on the second candidate", result.Status)
	}
	assertTrail(t, result.Trail, []AttemptTrace{
		{Upstream: "slow", CredentialIndex: 0, Status: http.StatusTooManyRequests},
		{Upstream: "fast", CredentialIndex: 0, Status: http.StatusOK},
	})
}

// TestTrailRecordsCredentialRotation: the ring walk inside one
// upstream shows up as two traces against the same upstream with
// different credentials.
func TestTrailRecordsCredentialRotation(t *testing.T) {
	t.Parallel()
	_, adapter := newUnauthorizedRing(t)
	exec := New(testPolicy(), nil, WithUpstreamFatalHook(func(_ string, cred int, _ string) {
		adapter.RetireCredential(cred)
	}))
	result := execute(t, exec, []upstream.Upstream{adapter}, false, `{}`)
	if result.Status != http.StatusOK {
		t.Fatalf("status = %d, want success on the second credential", result.Status)
	}
	assertTrail(t, result.Trail, []AttemptTrace{
		{Upstream: "ring", CredentialIndex: 0, Status: http.StatusUnauthorized},
		{Upstream: "ring", CredentialIndex: 1, Status: http.StatusOK},
	})
}

// TestTrailRecordsUncompletedAttempts: a breaker denial never
// completed an exchange — its trace names the upstream with no status.
func TestTrailRecordsUncompletedAttempts(t *testing.T) {
	t.Parallel()
	shut := jsonStubUpstream(t, "shut", http.StatusOK, `{"ok":true}`)
	healthy := jsonStubUpstream(t, "open", http.StatusOK, `{"ok":true}`)
	exec := New(testPolicy(), nil, WithBreaker(selectiveBreaker{deny: "shut"}))
	result := execute(t, exec, []upstream.Upstream{shut, healthy}, false, `{}`)
	if result.Status != http.StatusOK {
		t.Fatalf("status = %d, want success on the second candidate", result.Status)
	}
	assertTrail(t, result.Trail, []AttemptTrace{
		{Upstream: "shut", CredentialIndex: -1, Status: 0},
		{Upstream: "open", CredentialIndex: 0, Status: http.StatusOK},
	})
}

// assertTrail pins the recorded attempt trail element for element:
// same length, same upstream/credential/status sequence.
func assertTrail(t *testing.T, got, want []AttemptTrace) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("trail = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("trail[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}
}

// selectiveBreaker denies the named upstream and grants everything
// else, exercising the denial branch without a real state machine.
type selectiveBreaker struct{ deny string }

func (b selectiveBreaker) Allow(_ context.Context, id string) (circuit.Permission, bool) {
	if id == b.deny {
		return nil, false
	}
	return nopPermission{}, true
}

func (b selectiveBreaker) StateOf(context.Context, string) circuit.State { return circuit.StateClosed }

func (b selectiveBreaker) Reset(context.Context, string) {}
