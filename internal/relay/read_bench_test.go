/**
 * @file read_bench_test
 * @description Cost of the bounded upstream body read: the buffered
 * exchange buffers every non-streaming reply through readBounded, so
 * its allocation profile scales with the reply size. The first
 * benchmark pins the former unseeded growth chain (io.ReadAll) as the
 * comparison baseline for the pre-sized read.
 */
package relay

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"
)

func BenchmarkReadBoundedGrowthChainBaseline(b *testing.B) {
	for _, size := range []int{64 << 10, 4 << 20} {
		body := string(bytes.Repeat([]byte("x"), size))
		b.Run(sizeName(size), func(b *testing.B) {
			b.SetBytes(int64(size))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				// The former implementation: io.ReadAll's growth chain.
				if _, err := io.ReadAll(io.LimitReader(strings.NewReader(body), maxResponseBytes+1)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkReadBounded(b *testing.B) {
	for _, size := range []int{64 << 10, 4 << 20} {
		body := string(bytes.Repeat([]byte("x"), size))
		b.Run(sizeName(size)+"/hint", func(b *testing.B) {
			b.SetBytes(int64(size))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := readBounded(strings.NewReader(body), maxResponseBytes, int64(size)); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(sizeName(size)+"/nohint", func(b *testing.B) {
			b.SetBytes(int64(size))
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := readBounded(strings.NewReader(body), maxResponseBytes, -1); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func sizeName(size int) string {
	if size >= 1<<20 {
		return fmt.Sprintf("%dMiB", size>>20)
	}
	return fmt.Sprintf("%dKiB", size>>10)
}
