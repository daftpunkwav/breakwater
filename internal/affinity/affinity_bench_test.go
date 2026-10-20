/**
 * @file affinity_bench_test
 * @description Cost of the per-request Pick on the routing hot path,
 * at the node ceiling and below it, plus the chunk hashing the pick
 * front-runs. The at-cap case is the interesting one: a trie sitting
 * at its ceiling is steady state under sustained distinct traffic, so
 * whatever Pick costs there is what every request pays.
 */
package affinity

import (
	"strconv"
	"strings"
	"testing"
	"time"
)

// capSeededIndex returns an index whose model trie sits exactly at the
// node ceiling, every node fresh for the run (a TTL far past any
// benchmark). The seeding rule is the one TestNodeCeilingStopsRecording
// pins: distinct full-length chains, one per pick.
func capSeededIndex() *Index {
	idx := NewIndex(time.Hour, nil)
	for i := 0; i < maxNodes/maxChunks; i++ {
		idx.Pick("m", strings.Repeat(string(rune('A'+i)), maxChunks*chunkSize), []string{"a", "b"})
	}
	return idx
}

// BenchmarkPickAtCap measures one Pick against a trie at its node
// ceiling: the steady state a busy deployment sits in, and the case
// whose cost decides whether the ceiling degrades gracefully.
func BenchmarkPickAtCap(b *testing.B) {
	idx := capSeededIndex()
	prompt := strings.Repeat("z", chunkSize)
	ids := []string{"a", "b"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.Pick("m", prompt, ids)
	}
}

// BenchmarkPickAtCapDeepMatch measures the same Pick for a prompt the
// trie holds at full depth: ceiling cost plus the deepest possible
// match walk, the best case affinity can deliver.
func BenchmarkPickAtCapDeepMatch(b *testing.B) {
	idx := capSeededIndex()
	prompt := strings.Repeat("A", maxChunks*chunkSize)
	ids := []string{"a", "b"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.Pick("m", prompt, ids)
	}
}

// BenchmarkPickBelowCap measures the same Pick against a trie holding
// a single short chain — the cost profile of an index nowhere near its
// ceiling, for contrast with the at-cap cases.
func BenchmarkPickBelowCap(b *testing.B) {
	idx := NewIndex(time.Hour, nil)
	idx.Pick("m", "prefix", []string{"a", "b"})
	prompt := strings.Repeat("z", chunkSize)
	ids := []string{"a", "b"}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx.Pick("m", prompt, ids)
	}
}

// BenchmarkChunkHashes measures the prompt chunking that fronts every
// pick, at a chat-length and a long-context prompt size.
func BenchmarkChunkHashes(b *testing.B) {
	for _, size := range []int{4 << 10, 64 << 10} {
		prompt := strings.Repeat("w", size)
		b.Run(sizeLabel(size), func(b *testing.B) {
			b.SetBytes(int64(size))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = chunkHashes(prompt)
			}
		})
	}
}

func sizeLabel(size int) string {
	switch {
	case size >= 1<<20:
		return strconv.Itoa(size/(1<<20)) + "MB"
	case size >= 1<<10:
		return strconv.Itoa(size/(1<<10)) + "KB"
	default:
		return strconv.Itoa(size) + "B"
	}
}
