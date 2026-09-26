/**
 * @file fatal
 * @description Fatal upstream conditions: which completed error
 * exchanges prove an upstream broken beyond this request, rather than
 * transiently overloaded.
 *
 * Responsibilities:
 * - Classify one error exchange into a machine-readable fatal reason
 * - Nothing else: what happens to the upstream afterwards is the
 *   caller's policy (assembly wires the report to the routing
 *   switch's auto-disable)
 *
 * The rules stay deliberately narrow — an auto disable takes real
 * traffic down with it, so only deterministic failures qualify:
 *   - insufficient_quota (OpenAI reports it as a 429, so the status
 *     alone cannot tell it apart from a transient rate limit)
 *   - 401: the credentials are rejected
 *   - 403 carrying a provider-API error envelope (an API-level
 *     refusal; a 403 from a proxy or WAF interstitial carries none)
 */
package relay

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

// The fatal reasons reported through the fatal hook. They surface as
// metric labels and in the admin routing view.
const (
	reasonAuthFailure    = "upstream_auth_failure"
	reasonQuotaExhausted = "upstream_quota_exhausted"
)

// upstreamErrorBody is the lenient shape of an OpenAI-style error
// envelope: {"error":{"code":...,"type":...}}. Providers disagree on
// whether code is a string or a number, hence any.
type upstreamErrorBody struct {
	Error struct {
		Code any    `json:"code"`
		Type string `json:"type"`
	} `json:"error"`
}

// fatalUpstreamReason returns the fatal reason of one completed error
// exchange, or "" when the failure may be transient and deserves
// retry or failover instead.
func fatalUpstreamReason(status int, body []byte) string {
	var env upstreamErrorBody
	// A body that is not an OpenAI-style envelope (a proxy page, an
	// empty drain) leaves the zero struct: only the status rules apply.
	_ = json.Unmarshal(body, &env)
	code := strings.ToLower(strings.Trim(fmt.Sprint(env.Error.Code), `"`))
	typ := strings.ToLower(env.Error.Type)
	switch {
	case typ == "insufficient_quota" || code == "insufficient_quota":
		return reasonQuotaExhausted
	case status == http.StatusUnauthorized:
		return reasonAuthFailure
	case status == http.StatusForbidden && (typ != "" || env.Error.Code != nil):
		return reasonAuthFailure
	}
	return ""
}
