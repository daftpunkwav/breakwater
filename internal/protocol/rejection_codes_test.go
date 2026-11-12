/**
 * @file rejection_codes_test
 * @description The gateway failure vocabulary is closed: every code the
 * governance stages, the routing layer and the relay stamp into the
 * carrier and render into error envelopes is pinned here. Changing a
 * value or adding a code is a protocol change — the access log's
 * failure taxonomy and the admin insights read these exact strings.
 */
package protocol

import "testing"

// TestRejectionCodeValues pins the HTTP rejection vocabulary. The
// in-stream codes (sse.go) are pinned by the sse contract tests; this
// table pins the envelope codes the governance stages emit.
func TestRejectionCodeValues(t *testing.T) {
	cases := map[Code]string{
		CodeRateLimited:           "rate_limit_exceeded",
		CodeInsufficientQuota:     "insufficient_quota",
		CodeModelNotAllowed:       "model_not_allowed",
		CodeMissingAPIKey:         "missing_api_key",
		CodeInvalidAPIKey:         "invalid_api_key",
		CodeIdentityUnavailable:   "identity_unavailable",
		CodeConcurrencyLimited:    "concurrency_limit_exceeded",
		CodeQuotaNotProvisioned:   "quota_not_provisioned",
		CodeGovernanceUnavailable: "governance_unavailable",
	}
	for code, want := range cases {
		if string(code) != want {
			t.Errorf("code = %q, want the pinned value %q", string(code), want)
		}
	}
}

// TestGatewayFailureCodeValues pins the codes the relay and the
// inference handler render for gateway-originated failures and routing
// refusals. Budget exhaustion is deliberately absent: it shares
// CodeBudgetExhausted with the in-stream contract, pinned by the sse
// contract tests.
func TestGatewayFailureCodeValues(t *testing.T) {
	cases := map[Code]string{
		CodeNoUpstream:            "no_upstream",
		CodeCircuitOpen:           "circuit_open",
		CodeUpstreamSaturated:     "upstream_saturated",
		CodeUpstreamUnreachable:   "upstream_unreachable",
		CodeModelDisabled:         "model_disabled",
		CodeModelNotFound:         "model_not_found",
		CodeContextWindowExceeded: "context_window_exceeded",
		CodePipelineMisconfigured: "pipeline_misconfigured",
	}
	for code, want := range cases {
		if string(code) != want {
			t.Errorf("code = %q, want the pinned value %q", string(code), want)
		}
	}
}
