/**
 * @file admission_test
 * @description The TinyLFU-style admission gate: the doorkeeper keeps
 * one-hit wonders out of the sketch, counters saturate instead of
 * wrapping, the periodic halve decays evidence while preserving its
 * order, and the cache's capacity decision admits read-backed keys
 * over cold ones while updates bypass the gate entirely.
 */
package cache

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"
)

// TestAdmissionDoorkeeperSemantics: a first read only arms the
// doorkeeper (estimate 1), a second read enters the sketch (estimate
// 2), and an unread key estimates 0.
func TestAdmissionDoorkeeperSemantics(t *testing.T) {
	t.Parallel()
	a := newAdmission(16)
	h := a.hash("k")

	if got := a.estimate(h); got != 0 {
		t.Fatalf("fresh estimate = %d, want 0", got)
	}
	a.record(h)
	if got := a.estimate(h); got != 1 {
		t.Fatalf("estimate after first read = %d, want 1 (doorkeeper only)", got)
	}
	a.record(h)
	if got := a.estimate(h); got != 2 {
		t.Fatalf("estimate after second read = %d, want 2", got)
	}
	if got := a.estimate(a.hash("other")); got != 0 {
		t.Fatalf("unread key estimate = %d, want 0", got)
	}
}

// TestAdmissionSaturation: counters saturate instead of wrapping, so a
// hot key's estimate stays within the ceiling across any number of
// halve cycles (the exact value depends on where the cycle boundary
// lands; the doorkeeper's false positives add at most 1).
func TestAdmissionSaturation(t *testing.T) {
	t.Parallel()
	a := newAdmission(16)
	h := a.hash("hot")

	for i := 0; i < 200; i++ {
		a.record(h)
		for j := range a.rows {
			for k := range a.rows[j] {
				if hi, lo := a.rows[j][k]>>4, a.rows[j][k]&0x0f; hi > 15 || lo > 15 {
					t.Fatal("a 4-bit counter wrapped past its ceiling")
				}
			}
		}
	}
	if got := a.estimate(h); got > 16 {
		t.Fatalf("estimate = %d, want at most the ceiling 16 (15 sketch + 1 door)", got)
	}
}

// TestAdmissionHalveDecaysAndKeepsOrder: crossing the reset budget
// halves every sketch counter and clears the doorkeeper. The storage
// facts are checked directly — a hot key's counters halve from 3 to 1,
// every counter is at most halved, and the door bitmap ends zeroed —
// because estimate values themselves carry the doorkeeper's random
// false positives.
func TestAdmissionHalveDecaysAndKeepsOrder(t *testing.T) {
	t.Parallel()
	a := newAdmission(16)
	hot := a.hash("hot")

	for i := 0; i < 4; i++ {
		a.record(hot)
	}
	hotSum := 0
	for i := range a.rows {
		for j := range a.rows[i] {
			hotSum += int(a.rows[i][j]>>4) + int(a.rows[i][j]&0x0f)
		}
	}
	// The first read only arms the doorkeeper, so four records make
	// three sketch increments across the four rows.
	if hotSum != 3*sketchRows {
		t.Fatalf("hot counters sum = %d, want %d (3 increments across the rows)", hotSum, 3*sketchRows)
	}

	// Twelve one-hit keys push the increment count past the reset
	// budget (16); the last record triggers door clear + sketch halve.
	for i := 0; i < 12; i++ {
		a.record(a.hash(string(rune('a' + i))))
	}
	total, hotLeft := 0, 0
	for i := range a.rows {
		for j := range a.rows[i] {
			hi, lo := int(a.rows[i][j]>>4), int(a.rows[i][j]&0x0f)
			total += hi + lo
			hotLeft += hi + lo
		}
	}
	for i := 0; i < doorProbeCount; i++ {
		bit := a.doorBit(hot, uint64(i))
		if a.door[bit>>3]&(1<<(bit&7)) != 0 {
			t.Fatal("the doorkeeper must be cleared by the reset")
		}
	}
	// Every counter is at most halved: 4*4 ones before, at most 4*4/2
	// ones after, and the hot key's own four rows still carry exactly
	// one count each (3 halved to 1).
	if total > 4*sketchRows*15/16 {
		t.Fatalf("total counts = %d, want at most the halved budget", total)
	}
	if hotLeft != sketchRows {
		t.Fatalf("hot rows carry %d, want %d (3 halved to 1 per row)", hotLeft, sketchRows)
	}
}

// TestMemoryGateRejectsColdOverflow: a cache at capacity keeps its
// read-backed residents over a cold newcomer — the scan-pollution
// defense this gate exists for.
func TestMemoryGateRejectsColdOverflow(t *testing.T) {
	t.Parallel()
	m := NewMemory(WithCapacity(2))
	ctx := context.Background()

	_ = m.Set(ctx, "a", testEntry(), time.Hour)
	_ = m.Set(ctx, "b", testEntry(), time.Hour)
	// Read both residents repeatedly; the doorkeeper's first read
	// counts too, so this puts both estimates well above a cold key.
	for i := 0; i < 3; i++ {
		if _, err := m.Get(ctx, "a"); err != nil {
			t.Fatalf("get a: %v", err)
		}
		if _, err := m.Get(ctx, "b"); err != nil {
			t.Fatalf("get b: %v", err)
		}
	}

	// A cold newcomer loses to the weakest resident.
	_ = m.Set(ctx, "cold", testEntry(), time.Hour)
	if _, err := m.Get(ctx, "cold"); err == nil {
		t.Fatal("a cold key must not displace read-backed residents")
	}
	for _, key := range []string{"a", "b"} {
		if _, err := m.Get(ctx, key); err != nil {
			t.Fatalf("resident %s was evicted by a cold key", key)
		}
	}
}

// TestMemoryGateAdmitsReadBackedKey: a newcomer whose key keeps being
// read earns admission over an unread resident — misses feed the gate
// exactly like hits.
func TestMemoryGateAdmitsReadBackedKey(t *testing.T) {
	t.Parallel()
	m := NewMemory(WithCapacity(1))
	ctx := context.Background()

	_ = m.Set(ctx, "resident", testEntry(), time.Hour)
	// Read the absent key three times: the gate counts the misses.
	for i := 0; i < 3; i++ {
		if _, err := m.Get(ctx, "newcomer"); err == nil {
			t.Fatal("newcomer is not cached yet")
		}
	}
	_ = m.Set(ctx, "newcomer", testEntry(), time.Hour)
	if _, err := m.Get(ctx, "newcomer"); err != nil {
		t.Fatal("the read-backed newcomer must be admitted")
	}
	if _, err := m.Get(ctx, "resident"); err == nil {
		t.Fatal("the unread resident must give way")
	}
}

// TestMemoryUpdateBypassesGate: refreshing a resident key is always
// right, however full the cache is.
func TestMemoryUpdateBypassesGate(t *testing.T) {
	t.Parallel()
	m := NewMemory(WithCapacity(1))
	ctx := context.Background()

	_ = m.Set(ctx, "k", testEntry(), time.Hour)
	updated := testEntry()
	updated.Status = http.StatusTeapot
	if err := m.Set(ctx, "k", updated, time.Hour); err != nil {
		t.Fatalf("set: %v", err)
	}
	got, err := m.Get(ctx, "k")
	if err != nil || got.Status != http.StatusTeapot {
		t.Fatalf("get = %+v err = %v, want the updated entry", got, err)
	}
}

// TestMemoryGateConcurrentUse: reads feed the gate while writes make
// admission decisions; correctness is the race detector's job here.
func TestMemoryGateConcurrentUse(t *testing.T) {
	t.Parallel()
	m := NewMemory(WithCapacity(64))
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				key := "k" + string(rune('a'+j%32))
				_, _ = m.Get(ctx, key)
				_ = m.Set(ctx, key, testEntry(), time.Hour)
			}
		}(i)
	}
	wg.Wait()
}
