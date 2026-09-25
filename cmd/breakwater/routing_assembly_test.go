/**
 * @file routing_assembly_test
 * @description The routing assembly helpers: alias stripping to client
 * names, the switch's known-name sets, the tracker observer adapter
 * and the strategy gate in the serve path.
 */
package main

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/daftpunkwav/breakwater/internal/config"
	"github.com/daftpunkwav/breakwater/internal/router"
)

func TestClientModelsStripsAliases(t *testing.T) {
	t.Parallel()
	got := clientModels([]string{"gpt-4o=deepseek-chat", "deepseek-reasoner", "*"})
	want := []string{"gpt-4o", "deepseek-reasoner", "*"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Fatalf("clientModels = %v, want %v", got, want)
	}
}

func TestKnownModelsDeduplicatesAcrossUpstreams(t *testing.T) {
	t.Parallel()
	got := knownModels([]config.Upstream{
		{Models: []string{"m1", "alias=real"}},
		{Models: []string{"m1", "*"}},
	})
	// The wildcard stays out: it is a binding, not a disableable model.
	if len(got) != 2 || got[0] != "m1" || got[1] != "alias" {
		t.Fatalf("knownModels = %v, want m1, alias without duplicates or wildcard", got)
	}
}

func TestBuildBindingsResolvesClientNames(t *testing.T) {
	t.Parallel()
	bindings, err := buildBindings([]config.Upstream{{
		ID: "up", BaseURL: "http://x", Models: []string{"gpt-4o=deepseek-chat"},
	}})
	if err != nil || len(bindings) != 1 {
		t.Fatalf("bindings = %v err = %v", bindings, err)
	}
	if len(bindings[0].Models) != 1 || bindings[0].Models[0] != "gpt-4o" {
		t.Fatalf("binding models = %v, want the client-facing name only", bindings[0].Models)
	}
}

// TestTrackerObserverForwardsToRecord pins the adapter: the relay's
// outcome port lands in the router's tracker.
func TestTrackerObserverForwardsToRecord(t *testing.T) {
	t.Parallel()
	tr := router.NewTracker()
	trackerObserver{tr}.ObserveUpstream("u1", 20*time.Millisecond, false)

	if got := tr.Score("u1"); got != 20 {
		t.Fatalf("score = %v, want the reported 20ms", got)
	}
	trackerObserver{tr}.ObserveUpstream("u1", 0, true)
	if got := tr.Score("u1"); got <= 20 {
		t.Fatalf("score = %v, want the failure penalty applied", got)
	}
}

// TestServeRejectsUnknownStrategy: the serve path re-validates the
// routing strategy instead of trusting the environment stage.
func TestServeRejectsUnknownStrategy(t *testing.T) {
	t.Parallel()
	cfg := config.Config{Routing: config.Routing{Strategy: "psychic"}}
	err := serve(context.Background(), cfg, slog.New(slog.DiscardHandler), "test")
	if err == nil {
		t.Fatal("serve accepted an unknown routing strategy")
	}
}
