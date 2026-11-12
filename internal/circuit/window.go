/**
 * @file window
 * @description The rolling-window geometry shared by the ratio and
 * slow-call strategies.
 *
 * Responsibilities:
 * - Define, once, the window both strategies roll: forty buckets of
 *   250 milliseconds each — ten seconds of health evidence
 * - Nothing else: what each strategy counts inside its buckets is the
 *   strategy's own
 */
package circuit

import "time"

// The strategy windows share their geometry by construction: the
// bucket count and span are defined here, and each strategy's own
// constants derive from them, so the two windows cannot drift apart —
// a change to the evidence horizon reshapes both strategies together,
// which is the only way their readings stay comparable.
const (
	// windowBuckets is the bucket count of a strategy window.
	windowBuckets = 40
	// windowBucketSpan is the wall-clock width of one bucket; forty
	// spans of 250ms make the ten-second evidence horizon.
	windowBucketSpan = 250 * time.Millisecond
)
