/**
 * @file priority_test
 * @description Priority routing contract tests: binding order, the
 * wildcard, breaker-open exclusion, and the error split between an
 * unbound model and a model whose every upstream is circuit-open.
 */
package router

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/daftpunkwav/breakwater/internal/circuit"
	"github.com/daftpunkwav/breakwater/internal/upstream"
)

// stubUp names an upstream without behaving.
type stubUp struct{ id string }

func (s stubUp) ID() string { return s.id }

func (stubUp) Forward(context.Context, upstream.Request) (*upstream.Response, error) {
	return nil, errors.New("not used")
}

func (stubUp) Probe(context.Context) error { return nil }

func TestCandidatesExcludesBreakerOpen(t *testing.T) {
	t.Parallel()
	breaker := circuit.NewRegistry(circuit.Config{FailThreshold: 1, Cooldown: time.Hour})
	primary, fallback := stubUp{id: "primary"}, stubUp{id: "fallback"}
	priority, err := NewPriority([]Binding{
		{Models: []string{"m1"}, Upstream: primary},
		{Models: []string{"m1"}, Upstream: fallback},
	}, WithBreaker(breaker))
	if err != nil {
		t.Fatalf("router: %v", err)
	}
	ctx := context.Background()

	perm, ok := breaker.Allow(ctx, "primary")
	if !ok {
		t.Fatal("breaker denied in closed state")
	}
	perm.Report(circuit.OutcomeServerFault)

	candidates, err := priority.Candidates(ctx, "m1")
	if err != nil {
		t.Fatalf("candidates: %v", err)
	}
	if len(candidates) != 1 || candidates[0].ID() != "fallback" {
		t.Fatalf("candidates = %v, want only the fallback", candidateIDs(candidates))
	}
}

func TestCandidatesAllOpenIsErrUnavailable(t *testing.T) {
	t.Parallel()
	breaker := circuit.NewRegistry(circuit.Config{FailThreshold: 1, Cooldown: time.Hour})
	bound := stubUp{id: "u1"}
	priority, err := NewPriority([]Binding{{Models: []string{"m1"}, Upstream: bound}}, WithBreaker(breaker))
	if err != nil {
		t.Fatalf("router: %v", err)
	}
	ctx := context.Background()

	perm, _ := breaker.Allow(ctx, "u1")
	perm.Report(circuit.OutcomeServerFault)

	if _, err := priority.Candidates(ctx, "m1"); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v, want ErrUnavailable", err)
	}
}

func TestCandidatesUnknownModelIsNotErrUnavailable(t *testing.T) {
	t.Parallel()
	priority, err := NewPriority([]Binding{{Models: []string{"m1"}, Upstream: stubUp{id: "u1"}}})
	if err != nil {
		t.Fatalf("router: %v", err)
	}
	_, err = priority.Candidates(context.Background(), "other")
	if err == nil || errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v, want a plain no-binding failure", err)
	}
}

func candidateIDs(candidates []upstream.Upstream) []string {
	ids := make([]string, 0, len(candidates))
	for _, c := range candidates {
		ids = append(ids, c.ID())
	}
	return ids
}
