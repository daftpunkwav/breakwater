/**
 * @file admission
 * @description The TinyLFU-style admission gate for the in-process
 * cache: a count-min sketch with a doorkeeper front door estimates how
 * often a key has been read, and capacity pressure admits a new entry
 * only when its estimate beats the entry chosen for eviction.
 *
 * Responsibilities:
 * - Track read frequency per key hash (reads drive the evidence; writes
 *   never do — a rejected key that keeps being read earns admission)
 * - Answer one question: is this new key at least as frequent as the
 *   entry it would displace?
 * - Nothing else: storage, TTL and ownership live in memory.go
 *
 * Design notes:
 * - The doorkeeper (a small bloom filter) absorbs a key's first-ever
 *   read without touching the sketch, so one-hit wonders estimate 1 and
 *   only repeated keys accumulate count — the scan-pollution defense
 * - The sketch saturates at 15 per counter (wrapping would corrupt the
 *   LFU order) and periodically halves — every doorkeeper read plus a
 *   halve of every counter — so stale evidence decays exponentially and
 *   the gate tracks the recent workload instead of history
 * - Estimates compare with >=: a newcomer equal to the victim wins,
 *   because rejecting equals biases the cache toward ancient entries
 */
package cache

import (
	"hash/maphash"
	"sync"
)

// admission geometry: sketch depth is fixed; width scales with the
// tracked-key budget. The doorkeeper is ~10 bits per tracked key at a
// ~1% false-positive rate.
const (
	sketchRows      = 4
	nibblemask      = 0x77 // clears the high bit of each 4-bit counter on halve
	maxNibble       = 15
	doorBitsPerKey  = 10
	doorProbeCount  = 7
	estimateDoorBoo = 1
)

var admissionSeed = maphash.MakeSeed()

// admission estimates per-key read frequency. All methods are safe for
// concurrent use; the internal mutex is uncontended enough that the
// cache's read path can stay on its RLock.
type admission struct {
	mu      sync.Mutex
	rows    [sketchRows][]byte
	seeds   [sketchRows]uint64
	mask    uint64
	door    []byte
	doorM   uint64
	incrs   int64
	resetAt int64
}

// newAdmission builds the gate for a budget of tracked keys. The width
// rounds up to a power of two so masking replaces modulo.
func newAdmission(trackedKeys int) *admission {
	width := 1
	for width < trackedKeys {
		width <<= 1
	}
	a := &admission{
		mask:    uint64(width - 1),
		doorM:   uint64(doorBitsPerKey*width - 1),
		door:    make([]byte, (doorBitsPerKey*width+7)/8),
		resetAt: int64(width),
	}
	for i := range a.rows {
		a.rows[i] = make([]byte, (width+1)/2)
		a.seeds[i] = uint64(i)*0x9E3779B97F4A7C15 + 1
	}
	return a
}

// hash derives the gate's key hash. The gate owns the seed: a key's
// estimate is only compared against estimates of the same epoch.
func (a *admission) hash(key string) uint64 {
	return maphash.String(admissionSeed, key)
}

// record feeds one read into the gate. Misses count as much as hits: a
// key the gate rejected keeps earning its way in while callers keep
// asking for it. The doorkeeper absorbs a first-ever read (bits set,
// sketch untouched) so one-hit wonders estimate 1; only a repeat read
// enters the sketch.
func (a *admission) record(h uint64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.doorAdd(h) {
		a.sketchIncrement(h)
	}
	a.incrs++
	if a.incrs >= a.resetAt {
		a.incrs = 0
		a.doorReset()
		a.sketchHalve()
	}
}

// estimate returns the key's read frequency: the smallest of the sketch
// rows (their independent seeds decorrelate false positives) plus the
// doorkeeper's presence bit.
func (a *admission) estimate(h uint64) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.estimateLocked(h)
}

func (a *admission) estimateLocked(h uint64) int {
	min := byte(maxNibble)
	for i := range a.rows {
		if v := a.nibble((h^a.seeds[i])&a.mask, a.rows[i]); v < min {
			min = v
		}
	}
	if a.doorHas(h) {
		min += estimateDoorBoo
	}
	return int(min)
}

// nibble reads one 4-bit counter packed two per byte.
func (a *admission) nibble(idx uint64, row []byte) byte {
	return (row[idx>>1] >> ((idx & 1) * 4)) & 0x0f
}

// sketchIncrement bumps one 4-bit counter, saturating instead of
// wrapping: a counter that wraps would lie about frequency order.
func (a *admission) sketchIncrement(h uint64) {
	for i := range a.rows {
		idx := (h ^ a.seeds[i]) & a.mask
		cell := &a.rows[i][idx>>1]
		shift := (idx & 1) * 4
		if v := (*cell >> shift) & 0x0f; v < maxNibble {
			*cell += 1 << shift
		}
	}
}

// sketchHalve decays every counter by half, keeping the relative order:
// the gate tracks the recent workload, not all history.
func (a *admission) sketchHalve() {
	for i := range a.rows {
		row := a.rows[i]
		for j := range row {
			row[j] = (row[j] >> 1) & nibblemask
		}
	}
}

// doorAdd reports whether the hash was already present; a first
// presence only sets the bits and returns false, keeping one-hit
// wonders out of the sketch.
func (a *admission) doorAdd(h uint64) bool {
	present := true
	for i := 0; i < doorProbeCount; i++ {
		bit := a.doorBit(h, uint64(i))
		if a.door[bit>>3]&(1<<(bit&7)) == 0 {
			present = false
			break
		}
	}
	if present {
		return true
	}
	for i := 0; i < doorProbeCount; i++ {
		bit := a.doorBit(h, uint64(i))
		a.door[bit>>3] |= 1 << (bit & 7)
	}
	return false
}

// doorHas reports whether all probe bits are set.
func (a *admission) doorHas(h uint64) bool {
	for i := 0; i < doorProbeCount; i++ {
		bit := a.doorBit(h, uint64(i))
		if a.door[bit>>3]&(1<<(bit&7)) == 0 {
			return false
		}
	}
	return true
}

// doorBit derives one of the doorkeeper's probe positions from the
// high and low halves of the hash.
func (a *admission) doorBit(h uint64, probe uint64) uint64 {
	return (h + probe*(h>>32|1)) & a.doorM
}

// doorReset clears the doorkeeper: after a halve, the +1 presence bit
// would otherwise double-count keys whose counters just decayed.
func (a *admission) doorReset() {
	for i := range a.door {
		a.door[i] = 0
	}
}
