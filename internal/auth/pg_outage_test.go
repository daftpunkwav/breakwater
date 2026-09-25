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

// TestPGOutageLifecycle: Ping and Resolve fail loudly against a dead
// system of record, and Close releases the pool cleanly.
func TestPGOutageLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := NewPG(ctx, unreachableDSN)
	if err != nil {
		t.Fatalf("NewPG with a syntactically valid dsn: %v", err)
	}
	defer store.Close()

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
