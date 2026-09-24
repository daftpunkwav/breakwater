/**
 * @file eligibility
 * @description Cache eligibility: which parameter combinations may be
 * served from a cache at all.
 *
 * Responsibilities:
 * - Decide whether a request's semantics survive caching
 * - Nothing else: hashing and storage live with their owners
 *
 * Only deterministic combinations are eligible (PRD Q3): any sampling
 * parameter that would let the provider answer differently per call
 * bypasses the cache. Absent parameters keep the OpenAI defaults,
 * which are NOT deterministic (temperature defaults to 1) — so a
 * request is eligible only when determinism is explicit. Streaming is
 * orthogonal: an eligible stream request may be cached after
 * completion and replayed as a stream.
 */
package cache

import "github.com/daftpunkwav/breakwater/internal/protocol"

// Eligible reports whether the request may be served from / written to
// the cache. The body must already be byte-exact matched by the key;
// this check guards the semantics on top of the bytes.
func Eligible(req protocol.ChatRequest) bool {
	// temperature: must be explicitly zero.
	if req.Temperature == nil || *req.Temperature != 0 {
		return false
	}
	// top_p: must be explicitly 1 (the default, but requiring it
	// explicitly keeps the whitelist strict).
	if req.TopP != nil && *req.TopP != 1 {
		return false
	}
	// n: exactly one choice.
	if req.N != nil && *req.N != 1 {
		return false
	}
	return true
}
