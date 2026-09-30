/**
 * @file sharedfetch_test
 * @description Who owns a shared fetch: the request that starts the
 * flight does not own it. Covers a body too large to share, a starter
 * whose client disappears mid-flight, and what the starter itself is
 * still owed when the fetch yields nothing shareable.
 */
package cache

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/daftpunkwav/breakwater/internal/pipeline"
	"github.com/daftpunkwav/breakwater/internal/protocol"
)

func oversizedUpstream(fetches *atomic.Int64, release <-chan struct{}) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fetches.Add(1)
		<-release
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(strings.Repeat("x", maxCacheableBytes+1)))
	})
}

// TestCacheMiddlewareOversizedResponsesAreNeitherSharedNorStored locks
// the capture-cap contract on the singleflight path: a reply larger
// than the cap must not be published to waiters (they would replay a
// truncated prefix as a complete reply) and must not be stored (the
// prefix would come back on every later hit).
func TestCacheMiddlewareOversizedResponsesAreNeitherSharedNorStored(t *testing.T) {
	t.Parallel()
	var fetches atomic.Int64
	release := make(chan struct{})
	handler := pipeline.Chain(
		pipeline.CarrierStage(),
		pipeline.FormatStage(protocol.FormatOpenAIChat),
		Middleware(NewMemory(), NewFlight(), time.Minute, nil, time.Minute),
	)(oversizedUpstream(&fetches, release))

	body := `{"model":"m","temperature":0,"messages":[{"role":"user","content":"hi"}]}`

	ownerDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		ownerDone <- fireRequest(handler, body)
	}()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && fetches.Load() == 0 {
		time.Sleep(time.Millisecond)
	}

	waiterDone := make(chan *httptest.ResponseRecorder, 1)
	waiterIssued := make(chan struct{})
	go func() {
		// Signal just before firing, so the release below cannot
		// outrun the waiter on its way to the shared flight.
		close(waiterIssued)
		waiterDone <- fireRequest(handler, body)
	}()
	<-waiterIssued
	close(release)

	owner := <-ownerDone
	if owner.Code != http.StatusOK || owner.Body.Len() != maxCacheableBytes+1 {
		t.Fatalf("owner status = %d len = %d, want 200 and the full body", owner.Code, owner.Body.Len())
	}
	waiter := <-waiterDone
	if waiter.Code != http.StatusBadGateway || !strings.Contains(waiter.Body.String(), "upstream_unreachable") {
		t.Fatalf("waiter status = %d body = %s, want 502: a truncated capture must not be shared", waiter.Code, waiter.Body.String())
	}

	// Nothing was stored: the next request fetches upstream again and
	// gets its own complete reply.
	again := fireRequest(handler, body)
	if again.Code != http.StatusOK || again.Body.Len() != maxCacheableBytes+1 {
		t.Fatalf("after oversize: status = %d len = %d, want a fresh full fetch", again.Code, again.Body.Len())
	}
	if got := fetches.Load(); got != 2 {
		t.Fatalf("fetches = %d, want 2: the oversized reply must not be stored", got)
	}
}

// TestCacheMiddlewareWaiterSurvivesOwnerDisconnect pins the waiter's
// fate on a shared fetch whose owner's client walks away mid-flight:
// the owner writes nothing at all, the parked waiter never starts a
// fetch of its own, and when the flight ends without any response it
// fails with the 502 envelope instead of replaying a header-less
// capture.
func TestCacheMiddlewareWaiterSurvivesOwnerDisconnect(t *testing.T) {
	t.Parallel()
	entered := make(chan struct{})
	release := make(chan struct{})
	var fetches atomic.Int64
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fetches.Add(1)
		select {
		case entered <- struct{}{}:
		default:
		}
		<-release
		// The owner's client is gone: nothing is written at all.
	})
	flight := NewFlight()
	handler := pipeline.Chain(
		pipeline.CarrierStage(),
		pipeline.FormatStage(protocol.FormatOpenAIChat),
		Middleware(NewMemory(), flight, time.Minute, nil, time.Minute),
	)(upstream)

	body := `{"model":"m","temperature":0,"messages":[{"role":"user","content":"hi"}]}`
	ownerCtx, cancelOwner := context.WithCancel(context.Background())
	defer cancelOwner()

	ownerDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req.WithContext(ownerCtx))
		ownerDone <- rec
	}()
	<-entered // the flight entry exists from here on

	waiterDone := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		req := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		waiterDone <- rec
	}()

	// The shared fetch wait is the waiter's only blocking point; a
	// stability window without a second upstream fetch proves it is
	// parked there instead of fetching on its own.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if fetches.Load() != 1 {
			t.Fatal("waiter started its own fetch: it missed the shared flight")
		}
		select {
		case rec := <-waiterDone:
			t.Fatalf("waiter completed on its own: status %d", rec.Code)
		case <-time.After(300 * time.Millisecond):
		}
	}

	cancelOwner()
	close(release)
	<-ownerDone
	waiterRec := <-waiterDone
	if waiterRec.Code != http.StatusBadGateway || !strings.Contains(waiterRec.Body.String(), "upstream_unreachable") {
		t.Fatalf("waiter status = %d body = %s, want 502 envelope", waiterRec.Code, waiterRec.Body.String())
	}
}

// TestCacheSharedFetchSurvivesTheStarterLeaving pins the ownership
// rule of a shared fetch: the request that happens to start the flight
// does not own it. A starter that walks away mid-fetch must not cancel
// the upstream call, nor fail the requests still waiting on the same
// key, nor lose the entry they are entitled to.
func TestCacheSharedFetchSurvivesTheStarterLeaving(t *testing.T) {
	t.Parallel()
	var fetches atomic.Int64
	reached := make(chan struct{})
	release := make(chan struct{})
	handler := cacheStage(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fetches.Add(1)
		close(reached)
		// A real upstream exchange is bound by its context, which is
		// what makes the starter's cancellation matter.
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"shared"}}]}`))
	}))

	body := `{"model":"m","temperature":0,"messages":[{"role":"user","content":"hi"}]}`

	// The starter abandons its own request as soon as the fetch starts.
	starterCtx, cancelStarter := context.WithCancel(context.Background())
	starter := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	starter.Header.Set("Content-Type", "application/json")
	starter = starter.WithContext(starterCtx)
	starterRec := httptest.NewRecorder()
	go handler.ServeHTTP(starterRec, starter)
	<-reached
	cancelStarter()

	waiterRec := make(chan *httptest.ResponseRecorder, 1)
	waiterIssued := make(chan struct{})
	go func() {
		// Signal just before firing, so the release below cannot
		// outrun the waiter on its way to the shared flight.
		close(waiterIssued)
		waiterRec <- fireRequest(handler, body)
	}()
	<-waiterIssued
	close(release)

	waiter := <-waiterRec
	if waiter.Code != http.StatusOK || !strings.Contains(waiter.Body.String(), "shared") {
		t.Fatalf("waiter status = %d body = %s, want the shared fetch's 200", waiter.Code, waiter.Body.String())
	}
	if got := fetches.Load(); got != 1 {
		t.Fatalf("upstream fetches = %d, want exactly 1", got)
	}
}

// TestCacheSharedFetchRendersTheStarterAnEnvelope: when the shared
// fetch produced no response at all, the starter is owed one — nothing
// reached its connection through the tee. A fetch that did produce a
// response must never get a second one appended (the oversized case
// above covers that half).
func TestCacheSharedFetchRendersTheStarterAnEnvelope(t *testing.T) {
	t.Parallel()
	release := make(chan struct{})
	var started sync.WaitGroup
	started.Add(1)
	handler := cacheStage(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started.Done()
		<-release
		// Produce no response at all: the handler simply returns.
	}))

	body := `{"model":"m","temperature":0,"messages":[{"role":"user","content":"hi"}]}`
	rec := make(chan *httptest.ResponseRecorder, 1)
	go func() { rec <- fireRequest(handler, body) }()
	started.Wait()
	close(release)

	got := <-rec
	if got.Code != http.StatusBadGateway || !strings.Contains(got.Body.String(), "upstream_unreachable") {
		t.Fatalf("status = %d body = %s, want the 502 envelope a response-less fetch owes", got.Code, got.Body.String())
	}
}

// TestCacheSharedFetchStaysInsideItsBudget pins the bound of a shared
// fetch: detached from the starter's cancellation is not unbounded. A
// black-holed upstream must release the flight at the budget even when
// the retry policy contributes no deadline of its own (a deployment
// that sets the BREAKWATER_RETRY timeouts to zero), or the key can
// never start a new flight again.
func TestCacheSharedFetchStaysInsideItsBudget(t *testing.T) {
	t.Parallel()
	var fetches atomic.Int64
	var once sync.Once
	reached := make(chan struct{})
	budget := 100 * time.Millisecond
	handler := pipeline.Chain(
		pipeline.CarrierStage(),
		pipeline.FormatStage(protocol.FormatOpenAIChat),
		Middleware(NewMemory(), NewFlight(), time.Minute, nil, budget),
	)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fetches.Add(1)
		once.Do(func() { close(reached) })
		// A black hole: the exchange ends only when its context does.
		<-r.Context().Done()
	}))

	body := `{"model":"m","temperature":0,"messages":[{"role":"user","content":"hi"}]}`

	starterDone := make(chan *httptest.ResponseRecorder, 1)
	go func() { starterDone <- fireRequest(handler, body) }()
	<-reached

	// The fetch ends at the budget: the starter is owed the 502
	// envelope of a response-less fetch.
	var starter *httptest.ResponseRecorder
	select {
	case starter = <-starterDone:
	case <-time.After(5 * time.Second):
		t.Fatal("the shared fetch outlived its budget: the flight never ended")
	}
	if starter.Code != http.StatusBadGateway || !strings.Contains(starter.Body.String(), "upstream_unreachable") {
		t.Fatalf("starter status = %d body = %s, want the 502 envelope", starter.Code, starter.Body.String())
	}

	// The key is free again: a new request starts a new flight instead
	// of waiting on the dead one forever.
	secondDone := make(chan *httptest.ResponseRecorder, 1)
	go func() { secondDone <- fireRequest(handler, body) }()
	select {
	case second := <-secondDone:
		if second.Code != http.StatusBadGateway {
			t.Fatalf("second status = %d, want the same envelope", second.Code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the key never started a new flight: the dead flight still owns it")
	}
	if got := fetches.Load(); got != 2 {
		t.Fatalf("upstream fetches = %d, want 2: the first flight held the key past its budget", got)
	}
}

// TestNegativelyCacheableRules pins which failed exchanges are worth
// remembering. A gateway envelope is a transient state of this process,
// not a fact about the request, so caching it would make a circuit-open
// reply stick for the whole negative TTL.

type ctxHonoringStore struct{ Cache }

func (c ctxHonoringStore) Set(ctx context.Context, key string, entry Entry, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	return c.Cache.Set(ctx, key, entry, ttl)
}

// TestCacheStoresTheEntryTheLeavingStarterProduced: the storage write
// must not ride the starter's request. A starter that walked away
// mid-flight used to leave the entry unwritten, so every later request
// for that key paid for another upstream fetch — the waiters were
// served and the cache stayed cold.

func TestCacheStoresTheEntryTheLeavingStarterProduced(t *testing.T) {
	t.Parallel()
	var fetches atomic.Int64
	reached := make(chan struct{})
	release := make(chan struct{})
	store := ctxHonoringStore{NewMemory()}
	handler := pipeline.Chain(
		pipeline.CarrierStage(),
		pipeline.FormatStage(protocol.FormatOpenAIChat),
		Middleware(store, NewFlight(), time.Minute, nil, time.Minute),
	)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fetches.Add(1)
		close(reached)
		select {
		case <-release:
		case <-r.Context().Done():
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"choices":[{"message":{"content":"warm"}}]}`))
	}))

	body := `{"model":"m","temperature":0,"messages":[{"role":"user","content":"hi"}]}`
	starterCtx, cancelStarter := context.WithCancel(context.Background())
	starter := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", strings.NewReader(body))
	starter.Header.Set("Content-Type", "application/json")
	go handler.ServeHTTP(httptest.NewRecorder(), starter.WithContext(starterCtx))
	<-reached
	cancelStarter()
	close(release)

	// Give the flight a moment to finish and store before asserting.
	deadline := time.Now().Add(2 * time.Second)
	var entry Entry
	var found bool
	for time.Now().Before(deadline) {
		if e, err := store.Get(context.Background(), KeyFor([]byte(body))); err == nil {
			entry, found = e, true
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !found {
		t.Fatal("the entry the abandoned starter produced was never stored")
	}
	if !strings.Contains(string(entry.Body), "warm") {
		t.Fatalf("stored body = %q", entry.Body)
	}
	if got := fetches.Load(); got != 1 {
		t.Fatalf("upstream fetches = %d, want 1", got)
	}
}
