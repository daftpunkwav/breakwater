/**
 * @file singleflight_test
 * @description The single-fetch evidence under -race: concurrent cold-start
 * fetches of one key execute the loader exactly once, waiters ride the
 * shared result, and every waiter is bounded by its own context.
 */
package cache

import (
	"context"
	"errors"
	"net/http"
	"runtime"
	"strings"
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

// awaitFlight blocks for one event or fails the test after a generous
// deadline, so a broken flight surfaces as a failure instead of a hang.
func awaitFlight(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("%s never happened", what)
	}
}

// releaseFlight lets a parked fetch finish only after every caller has
// been issued AND registered as a waiter: it waits — under the registry
// lock — for each key's flight to hold its issued waiter count, then
// closes the hold inside the lock. Any caller that entered Do is by
// then its key's holder or a registered waiter, so a resumed loader
// serves a closed set and exactly one fetch covers everyone; a caller
// parked between its issue signal and the lookup cannot slip in as a
// second holder, no matter how the scheduler staggers it.
func releaseFlight(g *Flight, hold chan struct{}, issuedByKey map[string]int) {
	g.mu.Lock()
	defer g.mu.Unlock()
	for {
		ready := true
		for key, issued := range issuedByKey {
			c := g.calls[key]
			if c == nil || c.waiters.Load() < int64(issued) {
				ready = false
				break
			}
		}
		if ready {
			break
		}
		g.mu.Unlock()
		runtime.Gosched()
		g.mu.Lock()
	}
	close(hold)
}

func TestFlightSingleFetchUnderStampede(t *testing.T) {
	t.Parallel()
	g := NewFlight()
	var fetches atomic.Int64
	hold := make(chan struct{})
	entered := make(chan struct{})
	var once sync.Once

	const waiters = 64
	queued := make(chan struct{}, waiters)
	var wg sync.WaitGroup
	results := make(chan Entry, waiters)
	for range waiters {
		wg.Add(1)
		go func() {
			defer wg.Done()
			queued <- struct{}{}
			entry, _, _ := g.Do(context.Background(), "cold-key", func(context.Context) (Entry, error) {
				fetches.Add(1)
				// The flight is live and this loader is parked from
				// here on: every other caller deterministically rides it.
				once.Do(func() { close(entered) })
				<-hold // the fetch is slow: everyone must pile onto the flight
				return entryFor(200), nil
			})
			results <- entry
		}()
	}
	// Issue every caller before releasing the fetch — no wall-clock
	// guess about scheduler speed.
	for range waiters {
		<-queued
	}
	awaitFlight(t, entered, "the flight's registration")
	releaseFlight(g, hold, map[string]int{"cold-key": waiters - 1})
	wg.Wait()
	close(results)

	if got := fetches.Load(); got != 1 {
		t.Fatalf("fetches = %d, want exactly 1", got)
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
	entered := make(chan struct{})
	var once sync.Once
	boom := errors.New("upstream exploded")

	const callers = 8
	queued := make(chan struct{}, callers)
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			queued <- struct{}{}
			_, err, _ := g.Do(context.Background(), "k", func(context.Context) (Entry, error) {
				once.Do(func() { close(entered) })
				<-hold
				return Entry{}, boom
			})
			errs <- err
		}()
	}
	for range callers {
		<-queued
	}
	awaitFlight(t, entered, "the flight's registration")
	releaseFlight(g, hold, map[string]int{"k": callers - 1})
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

	// A separate goroutine owns the flight and stays in the loader;
	// entered fires on its first statement, so the flight's
	// registration is observed, not guessed.
	entered := make(chan struct{})
	ownerDone := make(chan struct{})
	go func() {
		defer close(ownerDone)
		_, _, _ = g.Do(context.Background(), "k", func(context.Context) (Entry, error) {
			close(entered)
			<-release
			return entryFor(200), nil
		})
	}()
	awaitFlight(t, entered, "the flight's registration")

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

// TestFlightPanicReleasesWaiters pins the resilience rule: a panicking
// holder must not wedge the flight. The waiters observe the failure,
// the panic still reaches the holder's own caller (net/http recovers it
// there), and the key is reusable afterwards.
func TestFlightPanicReleasesWaiters(t *testing.T) {
	t.Parallel()
	g := NewFlight()
	hold := make(chan struct{})
	entered := make(chan struct{})
	boom := errors.New("kaboom")

	// The holder panics inside fn; Do must release the flight and
	// re-raise toward its own caller.
	ownerGotPanic := make(chan any, 1)
	go func() {
		defer func() {
			ownerGotPanic <- recover()
		}()
		_, _, _ = g.Do(context.Background(), "k", func(context.Context) (Entry, error) {
			close(entered)
			<-hold
			panic(boom)
		})
	}()
	awaitFlight(t, entered, "the flight's registration")

	const waiters = 4
	queued := make(chan struct{}, waiters)
	var wg sync.WaitGroup
	errs := make(chan error, waiters)
	for range waiters {
		wg.Add(1)
		go func() {
			defer wg.Done()
			queued <- struct{}{}
			_, err, _ := g.Do(context.Background(), "k", func(context.Context) (Entry, error) {
				t.Error("a waiter must never execute the loader")
				return entryFor(200), nil
			})
			errs <- err
		}()
	}
	for range waiters {
		<-queued
	}
	releaseFlight(g, hold, map[string]int{"k": waiters})
	wg.Wait()
	close(errs)

	if r := <-ownerGotPanic; r != boom {
		t.Fatalf("recovered %v, want the original panic re-raised to the holder", r)
	}
	for err := range errs {
		if err == nil || !strings.Contains(err.Error(), "fetch panicked") {
			t.Fatalf("waiter err = %v, want the shared panic failure", err)
		}
	}
	// The flight must be gone: a fresh call owns its fetch again.
	_, err, owner := g.Do(context.Background(), "k", func(context.Context) (Entry, error) {
		return entryFor(200), nil
	})
	if !owner || err != nil {
		t.Fatalf("owner = %v err = %v, want the key reusable after the panic", owner, err)
	}
}

func TestFlightKeysAreIndependent(t *testing.T) {
	t.Parallel()
	g := NewFlight()
	var fetches atomic.Int64
	hold := make(chan struct{})
	enteredA := make(chan struct{})
	enteredB := make(chan struct{})
	var onceA, onceB sync.Once

	const callersPerKey = 4
	queued := make(chan struct{}, 2*callersPerKey)
	var wg sync.WaitGroup
	for _, key := range []string{"a", "b"} {
		for range callersPerKey {
			wg.Add(1)
			go func(k string) {
				defer wg.Done()
				queued <- struct{}{}
				_, _, _ = g.Do(context.Background(), k, func(context.Context) (Entry, error) {
					fetches.Add(1)
					switch k {
					case "a":
						onceA.Do(func() { close(enteredA) })
					case "b":
						onceB.Do(func() { close(enteredB) })
					}
					<-hold
					return entryFor(200), nil
				})
			}(key)
		}
	}
	// Both keys must produce exactly one fetch while the others wait:
	// wait for both flights to register and every caller to be issued,
	// then release — no wall-clock guess about late arrivals.
	awaitFlight(t, enteredA, "key a's flight registration")
	awaitFlight(t, enteredB, "key b's flight registration")
	for range 2 * callersPerKey {
		<-queued
	}
	releaseFlight(g, hold, map[string]int{"a": callersPerKey - 1, "b": callersPerKey - 1})
	wg.Wait()
	if got := fetches.Load(); got != 2 {
		t.Fatalf("fetches = %d, want 2 (one per key)", got)
	}
}
