/**
 * @file key
 * @description Cache key derivation from request bodies.
 *
 * Responsibilities:
 * - Hash a request body into a stable cache key
 * - Nothing else: request normalization (canonical encoding, parameter
 *   eligibility filtering) is a separate concern that feeds this function
 */
package cache

import (
	"crypto/sha256"
	"encoding/hex"
)

// KeyFor derives the cache key of a request body: the SHA-256 of its
// bytes. Matching is therefore byte-exact — two requests that differ
// only in JSON key order, spacing or an SDK's extra whitespace are
// different keys. The caller decides which parameter combinations are
// eligible at all; see Eligible.
func KeyFor(body []byte) string {
	sum := sha256.Sum256(body)
	return hex.EncodeToString(sum[:])
}
