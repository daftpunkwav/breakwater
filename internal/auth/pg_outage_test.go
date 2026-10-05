/**
 * @file pg_outage_test
 * @description The pg store against an unreachable database: assembly
 * still succeeds (the pool connects lazily), and health, resolution
 * and shutdown surface the outage instead of hanging or passing.
 */
package auth

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// unreachableDSN points at a port nothing listens on; the pool parses
// it fine and only fails when a connection is actually needed.
const unreachableDSN = "postgres://breakwater:secret@127.0.0.1:1/breakwater"

// newOutageStore assembles the store against the unreachable DSN under
// the given bound; assembly must succeed because the pool connects
// lazily. On the way out the store closes before the context is
// released — the same order the tests' own defers held.
func newOutageStore(t *testing.T, timeout time.Duration) (context.Context, *PGStore) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	t.Cleanup(cancel)
	store, err := NewPGStore(ctx, unreachableDSN)
	if err != nil {
		t.Fatalf("NewPGStore with a syntactically valid dsn: %v", err)
	}
	t.Cleanup(store.Close)
	return ctx, store
}

// TestPGOutageLifecycle: Ping and Resolve fail loudly against a dead
// system of record, and Close releases the pool cleanly.
func TestPGOutageLifecycle(t *testing.T) {
	ctx, store := newOutageStore(t, 10*time.Second)

	if err := store.Ping(ctx); err == nil {
		t.Fatal("Ping against a dead database must fail")
	}

	tenant, err := store.Resolve(ctx, "sk-1")
	if err == nil {
		t.Fatalf("Resolve against a dead database = (%+v, nil), want a failure", tenant)
	}
	if errors.Is(err, ErrUnauthorized) {
		t.Fatalf("err = %v, an outage must not surface as ErrUnauthorized", err)
	}
	if !strings.Contains(err.Error(), "resolve key") {
		t.Fatalf("err = %q, want the resolve-key wrap", err)
	}
}
