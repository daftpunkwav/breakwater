/**
 * @file attempt_trace_pin_test
 * @description The pin between the relay's attempt trail and the access
 * log's trail shape: attemptsToObs is the single conversion, and this
 * test fails when either struct gains a field the conversion does not
 * carry — the drift would otherwise surface as a silently missing log
 * column.
 */
package pipeline

import (
	"reflect"
	"testing"

	"github.com/daftpunkwav/breakwater/internal/obs"
	"github.com/daftpunkwav/breakwater/internal/relay"
)

func TestAttemptsToObsCarriesEveryField(t *testing.T) {
	t.Parallel()
	trail := []relay.AttemptTrace{
		{Upstream: "mock", CredentialIndex: 2, Status: 502},
		{Upstream: "fallback", CredentialIndex: -1, Status: 0},
	}
	got := attemptsToObs(trail)
	want := []obs.AttemptTrace{
		{Upstream: "mock", Credential: 2, Status: 502},
		{Upstream: "fallback", Credential: -1, Status: 0},
	}
	if len(got) != len(want) {
		t.Fatalf("trail = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("trail[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}

	// Shape pin: both structs carry exactly the three fields the
	// conversion maps (Upstream, CredentialIndex/Credential, Status).
	// A field added to either shape fails here and must be added to
	// attemptsToObs — or pinned as deliberately dropped — instead of
	// vanishing between the relay and the log.
	const trailFields = 3
	if n := reflect.TypeOf(relay.AttemptTrace{}).NumField(); n != trailFields {
		t.Errorf("relay.AttemptTrace has %d fields, want %d: extend attemptsToObs for any new field", n, trailFields)
	}
	if n := reflect.TypeOf(obs.AttemptTrace{}).NumField(); n != trailFields {
		t.Errorf("obs.AttemptTrace has %d fields, want %d: extend attemptsToObs for any new field", n, trailFields)
	}
}

// TestAttemptsToObsEmptyTrailIsNil pins the empty contract: a request
// that never reached an upstream logs no trail at all, not an empty
// list.
func TestAttemptsToObsEmptyTrailIsNil(t *testing.T) {
	t.Parallel()
	if got := attemptsToObs(nil); got != nil {
		t.Fatalf("empty trail = %v, want nil", got)
	}
}
