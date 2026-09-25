/**
 * @file priority_switch_test
 * @description The runtime switch wired into candidate selection: a
 * disabled model refuses, a disabled upstream drops out, and a fully
 * disabled model reads as unavailable.
 */
package router

import (
	"context"
	"errors"
	"testing"
)

func TestCandidatesDisabledModelIsErrDisabled(t *testing.T) {
	t.Parallel()
	sw := NewSwitch([]string{"m1"}, []string{"u1"})
	rt, err := NewPriority([]Binding{
		{Models: []string{"m1"}, Upstream: stubUp{id: "u1"}},
	}, WithSwitch(sw))
	if err != nil {
		t.Fatalf("router: %v", err)
	}

	if err := sw.SetModel("m1", false); err != nil {
		t.Fatalf("disable: %v", err)
	}
	if _, err := rt.Candidates(context.Background(), "m1"); !errors.Is(err, ErrDisabled) {
		t.Fatalf("err = %v, want ErrDisabled", err)
	}

	if err := sw.SetModel("m1", true); err != nil {
		t.Fatalf("re-enable: %v", err)
	}
	candidates, err := rt.Candidates(context.Background(), "m1")
	if err != nil || len(candidates) != 1 {
		t.Fatalf("candidates = %v err = %v, want the upstream back", candidateIDs(candidates), err)
	}
}

func TestCandidatesSkipDisabledUpstreams(t *testing.T) {
	t.Parallel()
	sw := NewSwitch([]string{"m1"}, []string{"u1", "u2"})
	rt, err := NewPriority([]Binding{
		{Models: []string{"m1"}, Upstream: stubUp{id: "u1"}},
		{Models: []string{"m1"}, Upstream: stubUp{id: "u2"}},
	}, WithSwitch(sw))
	if err != nil {
		t.Fatalf("router: %v", err)
	}

	if err := sw.SetUpstream("u1", false); err != nil {
		t.Fatalf("disable: %v", err)
	}
	candidates, err := rt.Candidates(context.Background(), "m1")
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	if len(candidates) != 1 || candidates[0].ID() != "u2" {
		t.Fatalf("candidates = %v, want only u2", candidateIDs(candidates))
	}

	// Both upstreams off is a health-style unavailability, not a
	// no-binding 404.
	if err := sw.SetUpstream("u2", false); err != nil {
		t.Fatalf("disable: %v", err)
	}
	if _, err := rt.Candidates(context.Background(), "m1"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v, want ErrUnavailable", err)
	}
}

func TestCandidatesDisabledWildcardModel(t *testing.T) {
	t.Parallel()
	sw := NewSwitch([]string{"any"}, []string{"u1"})
	rt, err := NewPriority([]Binding{
		{Models: []string{"*"}, Upstream: stubUp{id: "u1"}},
	}, WithSwitch(sw))
	if err != nil {
		t.Fatalf("router: %v", err)
	}

	// The wildcard's client-facing alias is the requested name itself;
	// disabling it gates every model this upstream would serve.
	if err := sw.SetModel("any", false); err != nil {
		t.Fatalf("disable: %v", err)
	}
	if _, err := rt.Candidates(context.Background(), "any"); !errors.Is(err, ErrDisabled) {
		t.Fatalf("err = %v, want ErrDisabled", err)
	}
}
