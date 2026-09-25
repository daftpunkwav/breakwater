/**
 * @file priority_construction_test
 * @description Router construction and option wiring: bindings are
 * validated at assembly time, a nil breaker disables the pre-filter,
 * and exact plus wildcard bindings compose into one candidate list.
 */
package router

import (
	"context"
	"errors"
	"strings"
	"testing"
)

func TestNewPriorityRejectsNilUpstream(t *testing.T) {
	t.Parallel()
	_, err := NewPriority([]Binding{{Models: []string{"m1"}, Upstream: nil}})
	if err == nil {
		t.Fatal("binding without an upstream accepted")
	}
	if !strings.Contains(err.Error(), "no upstream") {
		t.Fatalf("err = %v, want the missing-upstream message", err)
	}
}

func TestNewPriorityRejectsEmptyModels(t *testing.T) {
	t.Parallel()
	_, err := NewPriority([]Binding{{Models: nil, Upstream: stubUp{id: "u1"}}})
	if err == nil {
		t.Fatal("binding without models accepted")
	}
	if !strings.Contains(err.Error(), "lists no models") {
		t.Fatalf("err = %v, want the missing-models message", err)
	}
	if !strings.Contains(err.Error(), "u1") {
		t.Fatalf("err = %v, want the offending upstream named", err)
	}
}

// TestWithBreakerNilDisablesPrefilter pins the nil option: the router
// keeps every bound upstream as a candidate without consulting a
// breaker.
func TestWithBreakerNilDisablesPrefilter(t *testing.T) {
	t.Parallel()
	rt, err := NewPriority([]Binding{
		{Models: []string{"m1"}, Upstream: stubUp{id: "u1"}},
		{Models: []string{"*"}, Upstream: stubUp{id: "u2"}},
	}, WithBreaker(nil))
	if err != nil {
		t.Fatalf("router: %v", err)
	}

	candidates, err := rt.Candidates(context.Background(), "m1")
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	if len(candidates) != 2 {
		t.Fatalf("candidates = %v, want both upstreams", candidateIDs(candidates))
	}
}

// TestCandidatesCombinesExactAndWildcard pins the matching composition:
// an upstream bound to both the exact model and the wildcard contributes
// exactly once, binding order is preserved, and unbound models fall back
// to the wildcard holders only.
func TestCandidatesCombinesExactAndWildcard(t *testing.T) {
	t.Parallel()
	primary := stubUp{id: "exact-primary"}
	generalist := stubUp{id: "wildcard"}
	exactFallback := stubUp{id: "exact-fallback"}
	rt, err := NewPriority([]Binding{
		{Models: []string{"m1"}, Upstream: primary},
		{Models: []string{"*"}, Upstream: generalist},
		{Models: []string{"m1", "m2"}, Upstream: exactFallback},
	})
	if err != nil {
		t.Fatalf("router: %v", err)
	}

	candidates, err := rt.Candidates(context.Background(), "m1")
	if err != nil {
		t.Fatalf("candidates(m1): %v", err)
	}
	got := candidateIDs(candidates)
	want := []string{"exact-primary", "wildcard", "exact-fallback"}
	if len(got) != len(want) {
		t.Fatalf("candidates(m1) = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("candidates(m1) = %v, want %v", got, want)
		}
	}

	// A model without an exact binding still reaches the wildcard.
	candidates, err = rt.Candidates(context.Background(), "m9")
	if err != nil {
		t.Fatalf("candidates(m9): %v", err)
	}
	if ids := candidateIDs(candidates); len(ids) != 1 || ids[0] != "wildcard" {
		t.Fatalf("candidates(m9) = %v, want only the wildcard", ids)
	}
}

// TestCandidatesErrorNamesTheModel pins the no-binding failure shape:
// the error carries the requested model, and it is not ErrUnavailable.
func TestCandidatesErrorNamesTheModel(t *testing.T) {
	t.Parallel()
	rt, err := NewPriority([]Binding{{Models: []string{"m1"}, Upstream: stubUp{id: "u1"}}})
	if err != nil {
		t.Fatalf("router: %v", err)
	}

	_, err = rt.Candidates(context.Background(), "m2")
	if err == nil {
		t.Fatal("unbound model resolved")
	}
	if errors.Is(err, ErrUnavailable) {
		t.Fatal("a no-binding failure must not read as circuit-open")
	}
	if !strings.Contains(err.Error(), "m2") {
		t.Fatalf("err = %v, want the requested model named", err)
	}
}
