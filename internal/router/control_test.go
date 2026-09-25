/**
 * @file control_test
 * @description The runtime traffic switch: toggling known models and
 * upstreams, rejecting unknown names, fail-open reads and the view.
 */
package router

import (
	"errors"
	"sync"
	"testing"
)

func TestSwitchTogglesKnownNames(t *testing.T) {
	t.Parallel()
	s := NewSwitch([]string{"m1", "m2"}, []string{"u1"})

	if !s.ModelEnabled("m1") || !s.UpstreamEnabled("u1") {
		t.Fatal("everything is enabled before any toggle")
	}
	if err := s.SetModel("m1", false); err != nil {
		t.Fatalf("disable model: %v", err)
	}
	if s.ModelEnabled("m1") {
		t.Fatal("disabled model still reports enabled")
	}
	if err := s.SetModel("m1", true); err != nil {
		t.Fatalf("re-enable model: %v", err)
	}
	if !s.ModelEnabled("m1") {
		t.Fatal("re-enabled model still reports disabled")
	}
	if err := s.SetUpstream("u1", false); err != nil {
		t.Fatalf("disable upstream: %v", err)
	}
	if s.UpstreamEnabled("u1") {
		t.Fatal("disabled upstream still reports enabled")
	}
}

func TestSwitchRejectsUnknownNames(t *testing.T) {
	t.Parallel()
	s := NewSwitch([]string{"m1"}, []string{"u1"})

	if err := s.SetModel("typo", false); !errors.Is(err, ErrUnknownModel) {
		t.Fatalf("err = %v, want ErrUnknownModel", err)
	}
	if err := s.SetUpstream("typo", false); !errors.Is(err, ErrUnknownUpstream) {
		t.Fatalf("err = %v, want ErrUnknownUpstream", err)
	}
}

// TestSwitchReadsFailOpen: names the switch does not know read as
// enabled — the switch must not become a shadow model registry.
func TestSwitchReadsFailOpen(t *testing.T) {
	t.Parallel()
	s := NewSwitch([]string{"m1"}, []string{"u1"})
	if !s.ModelEnabled("never-configured") || !s.UpstreamEnabled("never-configured") {
		t.Fatal("unknown names must read as enabled")
	}
}

func TestSwitchViewListsEveryKnownName(t *testing.T) {
	t.Parallel()
	s := NewSwitch([]string{"m1", "m2"}, []string{"u1", "u2"})
	if err := s.SetModel("m2", false); err != nil {
		t.Fatalf("disable model: %v", err)
	}
	if err := s.SetUpstream("u2", false); err != nil {
		t.Fatalf("disable upstream: %v", err)
	}

	view := s.View()
	if !view.Models["m1"] || view.Models["m2"] {
		t.Fatalf("models view = %v, want m1 enabled and m2 disabled", view.Models)
	}
	if !view.Upstreams["u1"] || view.Upstreams["u2"] {
		t.Fatalf("upstreams view = %v, want u1 enabled and u2 disabled", view.Upstreams)
	}
}

// TestSwitchConcurrentToggleAndRead runs operators against request
// readers; under -race any unsynchronized access fails the test.
func TestSwitchConcurrentToggleAndRead(t *testing.T) {
	t.Parallel()
	s := NewSwitch([]string{"m1"}, []string{"u1"})

	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				_ = s.SetModel("m1", j%2 == 0)
				_ = s.SetUpstream("u1", j%2 == 0)
			}
		}()
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				_ = s.ModelEnabled("m1")
				_ = s.UpstreamEnabled("u1")
				_ = s.View()
			}
		}()
	}
	wg.Wait()
}
