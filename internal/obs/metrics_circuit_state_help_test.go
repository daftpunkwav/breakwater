/**
 * @file metrics_circuit_state_help_test
 * @description The published meaning of the breaker-state gauge: the
 * HELP line is the contract gauge readers parse ("0 closed, 1
 * half-open, 2 open") and the same mapping serve.go publishes through
 * stateValue and the admin surface reports as state strings. Changing a
 * state's value or wording without updating every reader breaks here.
 */
package obs

import (
	"bytes"
	"strings"
	"testing"
)

func TestCircuitStateGaugeHelpPinsStateValues(t *testing.T) {
	t.Parallel()
	var buf bytes.Buffer
	if err := NewMetrics().Render(&buf); err != nil {
		t.Fatalf("render: %v", err)
	}
	const want = "# HELP breakwater_circuit_state Breaker state (0 closed, 1 half-open, 2 open)."
	if !strings.Contains(buf.String(), want) {
		t.Fatalf("gauge help drifted from the published state mapping;\nwant line %q\nin:\n%s", want, buf.String())
	}
}
