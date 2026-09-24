/**
 * @file singleflight_test
 * @description The I2 evidence under -race: concurrent cold-start
 * fetches of one key execute the loader exactly once, waiters ride the
 * shared result, and every waiter is bounded by its own context.
 */
package cache

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func entryFor(status int) Entry {
	header := http.Header{}
	header.Set("Content-Type", "application/json")
	return Entry{Status: status, Header: header, Body: []byte("payload")}
}

func TestFlightSingleFetchUnderStampede(t *testing.T) {
	t.Parallel()
	g := NewFlight()
	var fetches atomic.Int64
	hold := make(chan struct{})

	const waiters = 64
	var wg sync.WaitGroup
	results := make(chan Entry, waiters)
	for range waiters {
		wg.Add(1)
		go func() {
			defer wg.Done()
			entry, _, _ := g.Do(context.Background(), "cold-key", func(context.Context) (Entry, error) {
				fetches.Add(1)
				<-hold // the fetch is slow: everyone must pile onto the flight
				return entryFor(200), nil
			})
			results <- entry
		}()
	}
	// Give the goroutines a moment to pile up, then release the fetch.
	time.Sleep(50 * time.Millisecond)
	close(hold)
	wg.Wait()
	close(results)

	if got := fetches.Load(); got != 1 {
		t.Fatalf("fetches = %d, want exactly 1 (I2)", got)
	}
	for entry := range results {
		if entry.Status != 200 || string(entry.Body) != "payload" {
			t.Fatalf("waiter got %+v, want the shared entry", entry)
		}
	}
}

func TestFlightOwnerFlagAndSequentialFetches(t *testing.T) {
	t.Parallel()
	g := NewFlight()
	calls := 0
	loader := func(context.Context) (Entry, error) {
		calls++
		return entryFor(200), nil
	}
	if _, _, owner := g.Do(context.Background(), "k", loader); !owner {
		t.Fatal("first caller must own the fetch")
	}
	if _, _, owner := g.Do(context.Background(), "k", loader); !owner {
		t.Fatal("after completion a new call must own its fetch")
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2: completed flights are not reused", calls)
	}
}

func TestFlightErrorsSharedWithoutRetry(t *testing.T) {
	t.Parallel()
	g := NewFlight()
	hold := make(chan struct{})
	boom := errors.New("upstream exploded")

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err, _ := g.Do(context.Background(), "k", func(context.Context) (Entry, error) {
				<-hold
				return Entry{}, boom
			})
			errs <- err
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(hold)
	wg.Wait()
	close(errs)

	for err := range errs {
		if !errors.Is(err, boom) {
			t.Fatalf("waiter err = %v, want the holder's error shared", err)
		}
	}
}

func TestFlightWaiterContextBoundsTheWait(t *testing.T) {
	t.Parallel()
	g := NewFlight()
	release := make(chan struct{})
	defer close(release)

	// A separate goroutine owns the flight and stays in the loader.
	ownerDone := make(chan struct{})
	go func() {
		defer close(ownerDone)
		g.Do(context.Background(), "k", func(context.Context) (Entry, error) {
			<-release
			return entryFor(200), nil
		})
	}()
	time.Sleep(50 * time.Millisecond) // the flight registers

	waiterCtx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err, owner := g.Do(waiterCtx, "k", func(context.Context) (Entry, error) {
		t.Error("the waiter must never execute the loader")
		<-release
		return entryFor(200), nil
	})
	if owner {
		t.Fatal("the waiter must not own the fetch")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want the waiter's own deadline", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("waiter blocked %v, its own context should have ended it", elapsed)
	}
}

func TestFlightKeysAreIndependent(t *testing.T) {
	t.Parallel()
	g := NewFlight()
	var fetches atomic.Int64
	hold := make(chan struct{})
	defer close(hold)

	for _, key := range []string{"a", "b"} {
		for range 4 {
			go func(k string) {
				g.Do(context.Background(), k, func(context.Context) (Entry, error) {
					fetches.Add(1)
					<-hold
					return entryFor(200), nil
				})
			}(key)
		}
	}
	// Both keys must produce exactly one fetch while the others wait.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if fetches.Load() >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond) // late arrivals pile onto the flights
	if got := fetches.Load(); got != 2 {
		t.Fatalf("fetches = %d, want 2 (one per key)", got)
	}
}
