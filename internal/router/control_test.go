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

// TestSwitchWildcardModelsToggleOnDemand: under a wildcard binding the
// concrete names arrive unenumerated, so a named toggle lands on the
// model the wildcard serves — and the disabled state stays visible in
// the view even though no binding ever named it.
func TestSwitchWildcardModelsToggleOnDemand(t *testing.T) {
	t.Parallel()
	s := NewSwitch(nil, []string{"u1"}, WithWildcardModels(true))

	if err := s.SetModel("gpt-4o", false); err != nil {
		t.Fatalf("SetModel under a wildcard: %v", err)
	}
	if s.ModelEnabled("gpt-4o") {
		t.Fatal("a wildcard-disabled model must read as disabled")
	}
	if err := s.SetModel("gpt-4o", true); err != nil {
		t.Fatalf("re-enable: %v", err)
	}
	if !s.ModelEnabled("gpt-4o") {
		t.Fatal("a re-enabled model must read as enabled")
	}

	// The typo protection still guards non-wildcard deployments.
	strict := NewSwitch(nil, []string{"u1"}, WithWildcardModels(false))
	if err := strict.SetModel("typo", false); !errors.Is(err, ErrUnknownModel) {
		t.Fatalf("err = %v, want ErrUnknownModel without a wildcard", err)
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

func TestAutoDisableTakesUpstreamOutOfRotation(t *testing.T) {
	t.Parallel()
	s := NewSwitch([]string{"m1"}, []string{"u1"})

	newly, err := s.AutoDisableUpstream("u1", "upstream_auth_failure")
	if err != nil || !newly {
		t.Fatalf("AutoDisableUpstream = %v, %v; want a fresh transition", newly, err)
	}
	if s.UpstreamEnabled("u1") {
		t.Fatal("auto-disabled upstream still reports enabled")
	}

	// A repeat observation is absorbed: the transition already happened.
	newly, err = s.AutoDisableUpstream("u1", "upstream_auth_failure")
	if err != nil || newly {
		t.Fatalf("repeat AutoDisableUpstream = %v, %v; want absorbed", newly, err)
	}

	// Auto-disable only records its own channel; the model switch and
	// the manual channel stay untouched.
	if err := s.AutoEnableUpstream("u1"); err != nil {
		t.Fatalf("AutoEnableUpstream: %v", err)
	}
	if !s.UpstreamEnabled("u1") {
		t.Fatal("recovered upstream still reports disabled")
	}
}

func TestAutoDisableUnknownUpstreamFailsClosed(t *testing.T) {
	t.Parallel()
	s := NewSwitch([]string{"m1"}, []string{"u1"})
	if _, err := s.AutoDisableUpstream("typo", "upstream_auth_failure"); !errors.Is(err, ErrUnknownUpstream) {
		t.Fatalf("err = %v, want ErrUnknownUpstream", err)
	}
	if err := s.AutoEnableUpstream("typo"); !errors.Is(err, ErrUnknownUpstream) {
		t.Fatalf("err = %v, want ErrUnknownUpstream", err)
	}
}

// TestManualDecisionSubsumesAuto: an operator disable must not be
// downgraded to an auto disable, and an operator enable must lift a
// recorded auto disable — human intent wins in both directions.
func TestManualDecisionSubsumesAuto(t *testing.T) {
	t.Parallel()
	s := NewSwitch([]string{"m1"}, []string{"u1"})

	if err := s.SetUpstream("u1", false); err != nil {
		t.Fatalf("manual disable: %v", err)
	}
	newly, err := s.AutoDisableUpstream("u1", "upstream_quota_exhausted")
	if err != nil || newly {
		t.Fatalf("AutoDisableUpstream over manual = %v, %v; want absorbed", newly, err)
	}

	// Operator enable lifts everything.
	if err := s.SetUpstream("u1", true); err != nil {
		t.Fatalf("manual enable: %v", err)
	}
	if !s.UpstreamEnabled("u1") {
		t.Fatal("operator-enabled upstream still disabled")
	}
	if ids := s.AutoDisabledIDs(); len(ids) != 0 {
		t.Fatalf("AutoDisabledIDs = %v, want none after an operator enable", ids)
	}

	// And a manual disable clears a stale auto record, so a later
	// re-enable restores service without a separate auto-enable.
	if _, err := s.AutoDisableUpstream("u1", "upstream_auth_failure"); err != nil {
		t.Fatalf("auto disable: %v", err)
	}
	if err := s.SetUpstream("u1", false); err != nil {
		t.Fatalf("manual disable: %v", err)
	}
	if err := s.SetUpstream("u1", true); err != nil {
		t.Fatalf("manual enable: %v", err)
	}
	if !s.UpstreamEnabled("u1") {
		t.Fatal("upstream still disabled after the manual cycle lifted the auto record")
	}
}

// TestAutoDisabledIDsSortedAndDisabled: the prober iterates the
// auto-disabled set, so it must list exactly the ineligible upstreams
// in a deterministic order.
func TestAutoDisabledIDsSortedAndDisabled(t *testing.T) {
	t.Parallel()
	s := NewSwitch([]string{"m1"}, []string{"ub", "ua", "uc", "ud"})
	if err := s.SetUpstream("ud", false); err != nil {
		t.Fatalf("manual disable: %v", err)
	}
	for _, id := range []string{"ub", "ua", "uc"} {
		if _, err := s.AutoDisableUpstream(id, "upstream_auth_failure"); err != nil {
			t.Fatalf("auto disable %s: %v", id, err)
		}
	}
	ids := s.AutoDisabledIDs()
	if len(ids) != 3 || ids[0] != "ua" || ids[1] != "ub" || ids[2] != "uc" {
		t.Fatalf("AutoDisabledIDs = %v, want the three auto-disabled, sorted", ids)
	}
}

func TestSwitchViewCarriesAutoDisableReasons(t *testing.T) {
	t.Parallel()
	s := NewSwitch([]string{"m1"}, []string{"u1", "u2"})
	if _, err := s.AutoDisableUpstream("u2", "upstream_quota_exhausted"); err != nil {
		t.Fatalf("auto disable: %v", err)
	}

	view := s.View()
	if !view.Upstreams["u1"] || view.Upstreams["u2"] {
		t.Fatalf("upstreams view = %v, want u1 enabled and u2 out", view.Upstreams)
	}
	info, ok := view.Auto["u2"]
	if !ok {
		t.Fatalf("auto view = %v, want an entry for u2", view.Auto)
	}
	if info.Reason != "upstream_quota_exhausted" {
		t.Fatalf("reason = %q, want upstream_quota_exhausted", info.Reason)
	}
	if info.Since.IsZero() {
		t.Fatal("since is zero; the decision moment must be recorded")
	}
	if _, ok := view.Auto["u1"]; ok {
		t.Fatal("healthy u1 must not appear in the auto view")
	}
}

// TestSwitchFiresEnableCallback: an operator enable and a lifted auto
// disable both fire the enable callback — the hook that restores the
// upstream's credential ring — while disables stay silent and a
// failing-closed unknown name never reaches it.
func TestSwitchFiresEnableCallback(t *testing.T) {
	t.Parallel()
	sw := NewSwitch([]string{"m1"}, []string{"u1"})
	var enabled []string
	sw.OnUpstreamEnable = func(id string) { enabled = append(enabled, id) }

	// Disables stay silent.
	if err := sw.SetUpstream("u1", false); err != nil {
		t.Fatalf("disable: %v", err)
	}
	if len(enabled) != 0 {
		t.Fatalf("enables = %v, want none after a disable", enabled)
	}
	// The operator enable fires.
	if err := sw.SetUpstream("u1", true); err != nil {
		t.Fatalf("enable: %v", err)
	}
	if len(enabled) != 1 || enabled[0] != "u1" {
		t.Fatalf("enables = %v, want u1 once", enabled)
	}
	// A lifted auto disable fires too.
	if _, err := sw.AutoDisableUpstream("u1", "upstream_auth_failure"); err != nil {
		t.Fatalf("auto disable: %v", err)
	}
	if err := sw.AutoEnableUpstream("u1"); err != nil {
		t.Fatalf("auto enable: %v", err)
	}
	if len(enabled) != 2 || enabled[1] != "u1" {
		t.Fatalf("enables = %v, want u1 twice", enabled)
	}
	// An unknown name fails closed without firing.
	if err := sw.SetUpstream("typo", true); err == nil {
		t.Fatal("unknown upstream accepted")
	}
	if len(enabled) != 2 {
		t.Fatalf("enables = %v, want unchanged for an unknown name", enabled)
	}
}
