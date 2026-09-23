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

// KeyFor derives the cache key of a request body. The input must already
// be normalized; normalization rules, including which parameter
// combinations are cache-eligible, live with the caller.
func KeyFor(normalizedBody []byte) string {
	sum := sha256.Sum256(normalizedBody)
	return hex.EncodeToString(sum[:])
}
