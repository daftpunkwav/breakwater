/**
 * @file affinity
 * @description Prompt-prefix affinity: remembers which upstream
 * recently served which prompt prefixes, and promotes the upstream
 * holding the longest match to the head of the candidate list. A
 * provider's prompt cache warms per prefix, so repeating a prefix on
 * the upstream that already holds it saves the prefill work the
 * first request paid for.
 *
 * Responsibilities:
 * - Keep one prefix trie per model: the prompt text cut into
 *   fixed-size chunks, each chunk hashed to a key, every node holding
 *   the upstream IDs that were routed through that prefix
 * - Match a request's chunk chain against the trie, deepest match
 *   winning, ties at one depth resolved by the candidate order the
 *   router produced
 * - Record the routing decision itself at pick time, misses
 *   included — an index that only recorded matches would never seed
 * - Nothing else: candidate eligibility (operator switches, breaker
 *   state) stays with the router; the index only ever sees IDs the
 *   caller already cleared, so a dead or excluded upstream can never
 *   pin traffic
 *
 * Memory is bounded twice over: a per-model node ceiling, and a
 * freshness TTL enforced by a throttled sweep on the write path (the
 * same discipline the cache module's expiry uses) — a node no insert
 * or match has touched within the TTL is pruned oldest-first.
 */
package affinity

import (
	"hash/fnv"
	"sync"
	"time"
)

// Chunk geometry: the prompt text is cut into fixed byte slices, each
// hashed independently. Fixed chunks need no per-node text and no
// insert-time edge splitting; two prompts align on the same chunk
// boundaries because chunking always starts at position zero. The
// trailing short slice is hashed as-is, so a conversation that
// continues past a recorded request's end diverges by at most one
// chunk of affinity.
const (
	chunkSize = 128
	// maxChunks bounds the per-request work. The head of the prompt
	// dominates upstream cache reuse, and the tail of a long
	// conversation is where requests diverge anyway.
	maxChunks = 512
	// maxNodes caps one model's trie. A record that would exceed the
	// ceiling is skipped after a sweep — affinity quietly degrades to
	// plain strategy order instead of growing without bound.
	maxNodes = 8192
)

// node is one prefix depth in a trie: one chunk hash further from the
// root. endpoints holds the upstream IDs routed through this prefix;
// touched is the last insert or match that walked through. The maps
// stay nil until first use — reads on a nil map are safe.
type node struct {
	children  map[uint64]*node
	endpoints map[string]struct{}
	touched   time.Time
}

// trie is one model's prefix tree. nodes counts the nodes under root
// for the ceiling check.
type trie struct {
	root  *node
	nodes int
}

// Index records prompt prefixes per model. Build one with NewIndex;
// Pick is safe for concurrent use. A nil *Index is valid and matches
// nothing, so disabled affinity needs no branch at the call sites.
type Index struct {
	mu     sync.Mutex
	models map[string]*trie
	// now returns the clock the TTL is judged on; injectable for tests.
	now func() time.Time
	// ttl is how long a node stays fresh past its last touch.
	ttl time.Duration
	// sweptAt arms the throttled sweep.
	sweptAt time.Time
}

// NewIndex builds an index whose entries expire after ttl. A
// non-positive ttl returns nil: the feature is off, and the call
// sites keep the router's order untouched.
func NewIndex(ttl time.Duration, now func() time.Time) *Index {
	if ttl <= 0 {
		return nil
	}
	if now == nil {
		now = time.Now
	}
	return &Index{models: make(map[string]*trie), now: now, ttl: ttl}
}

// Pick matches the prompt's chunk chain against the model's trie and
// returns the position in ids of the upstream to promote to the head
// of the candidate list — -1 when the router's order should stand.
// ids must be the eligibility-filtered candidates in router order.
// The decision is recorded either way: a match records the matched
// upstream, a miss records the caller's first candidate, so the next
// identical request can match. Matching and recording share one lock,
// so concurrent identical requests cannot interleave a match with a
// competing record.
func (x *Index) Pick(model, prompt string, ids []string) int {
	if x == nil || len(ids) == 0 {
		return -1
	}
	chunks := chunkHashes(prompt)
	x.mu.Lock()
	defer x.mu.Unlock()
	now := x.now()
	t := x.models[model]
	if t == nil {
		t = &trie{root: &node{touched: now}}
		x.models[model] = t
	}
	if x.sweptAt.IsZero() || now.Sub(x.sweptAt) >= x.ttl/2 || t.nodes >= maxNodes {
		x.sweep(now)
	}
	chosen := t.match(chunks, ids, now)
	if chosen < 0 {
		chosen = 0
	}
	t.record(chunks, ids[chosen], now)
	if chosen == 0 {
		return -1
	}
	return chosen
}

// match walks the chunk chain, keeping the eligible ID with the
// lowest router position at each depth. The walk stops at the first
// depth whose node is missing or holds none of ids; the deepest hit
// wins, so a longer shared prefix outranks the router's own
// preference among shallower hits. Node freshness is not judged here:
// a node expires through Index.sweep, which the write path drives.
func (t *trie) match(chunks []uint64, ids []string, now time.Time) int {
	pos := make(map[string]int, len(ids))
	for i, id := range ids {
		pos[id] = i
	}
	n := t.root
	best := -1
	for _, h := range chunks {
		child := n.children[h]
		if child == nil {
			break
		}
		n = child
		n.touched = now
		hit := -1
		for id := range child.endpoints {
			if i, ok := pos[id]; ok && (hit < 0 || i < hit) {
				hit = i
			}
		}
		if hit < 0 {
			break
		}
		best = hit
	}
	return best
}

// record inserts the chunk chain under id, creating missing nodes and
// stamping every node it walks. It is the routing decision's echo,
// not a completion report: a failed exchange leaves its entry in
// place, and the next Pick's eligibility filter is what keeps a dead
// upstream unpinnable. A chain cut short by the node ceiling stays a
// valid shorter prefix.
func (t *trie) record(chunks []uint64, id string, now time.Time) {
	n := t.root
	n.touched = now
	for _, h := range chunks {
		child := n.children[h]
		if child == nil {
			if t.nodes >= maxNodes {
				return
			}
			if n.children == nil {
				n.children = make(map[uint64]*node)
			}
			child = &node{touched: now}
			n.children[h] = child
			t.nodes++
		}
		if child.endpoints == nil {
			child.endpoints = make(map[string]struct{})
		}
		child.endpoints[id] = struct{}{}
		child.touched = now
		n = child
	}
}

// sweep prunes every trie's nodes untouched for ttl. It runs on the
// write path at most once per half-TTL — the same throttled-sweep
// discipline the cache module uses — plus whenever a trie sits at its
// node ceiling and a record needs room.
func (x *Index) sweep(now time.Time) {
	for _, t := range x.models {
		x.prune(t, t.root, now)
	}
	x.sweptAt = now
}

// prune deletes the subtrees of n that expired, reporting whether n
// itself is now an expired leaf. Children go first, so a long-shared
// prefix survives while only its tail extensions are stale, and
// becomes prunable itself once nothing live hangs below it.
func (x *Index) prune(t *trie, n *node, now time.Time) bool {
	for h, child := range n.children {
		if x.prune(t, child, now) {
			delete(n.children, h)
			t.nodes--
		}
	}
	return len(n.children) == 0 && now.Sub(n.touched) > x.ttl
}

// chunkHashes cuts the prompt into fixed byte chunks and hashes each
// with FNV-1a 64: deterministic and unkeyed, and collision-tolerant —
// a collision merges two chunk chains and can only route a request to
// an upstream that serves the model anyway. Prompts longer than the
// chunk budget use their head; the tail is where conversations
// diverge.
func chunkHashes(prompt string) []uint64 {
	if len(prompt) > maxChunks*chunkSize {
		prompt = prompt[:maxChunks*chunkSize]
	}
	if len(prompt) == 0 {
		return nil
	}
	chunks := make([]uint64, 0, (len(prompt)+chunkSize-1)/chunkSize)
	for offset := 0; offset < len(prompt); offset += chunkSize {
		end := offset + chunkSize
		if end > len(prompt) {
			end = len(prompt)
		}
		h := fnv.New64a()
		h.Write([]byte(prompt[offset:end]))
		chunks = append(chunks, h.Sum64())
	}
	return chunks
}
