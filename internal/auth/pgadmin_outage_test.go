/**
 * @file pgadmin_outage_test
 * @description The administration port against a dead system of
 * record: every operation fails loudly as a wrapped error, never as a
 * silent success. Also covers the key and id generators.
 */
package auth

import (
	"context"
	"strings"
	"testing"
	"time"
)

// TestGenerateKeyShape pins the identifier and key generators; the
// role validation sits before any database contact.
func TestGenerateKeyShape(t *testing.T) {
	t.Parallel()
	a, err := GenerateKey()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	b, err := GenerateKey()
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	if !strings.HasPrefix(a, "bw-") || len(a) != len("bw-")+48 {
		t.Fatalf("key = %q, want bw- plus 24 hex bytes", a)
	}
	if a == b {
		t.Fatal("two generated keys collided")
	}
}

// TestCreateUserRejectsUnknownRole: the role check precedes any
// database exchange, so a bad role fails even against a dead store.
func TestCreateUserRejectsUnknownRole(t *testing.T) {
	t.Parallel()
	store, err := NewPG(context.Background(), unreachableDSN)
	if err != nil {
		t.Fatalf("NewPG: %v", err)
	}
	defer store.Close()
	if _, err := store.CreateUser(context.Background(), "Alice", Role("owner"), "free"); err == nil || strings.Contains(err.Error(), "dial") {
		t.Fatalf("err = %v, want the role validation, not a dial failure", err)
	}
}

// TestRandomIDShape pins the identifier generator the pg store uses.
func TestRandomIDShape(t *testing.T) {
	t.Parallel()
	id, err := randomID(keyIDPrefix)
	if err != nil {
		t.Fatalf("randomID: %v", err)
	}
	if !strings.HasPrefix(id, keyIDPrefix) || len(id) != len(keyIDPrefix)+24 {
		t.Fatalf("id = %q, want the prefix plus 24 hex chars", id)
	}
	if hexEncode([]byte{0x0f, 0xa0}) != "0fa0" {
		t.Fatal("hexEncode mangled its input")
	}
}

// TestPGAdminOutageFailsLoudly: with the database unreachable every
// administration operation returns an error (the wrapped transport
// failure), and nothing pretends to have succeeded.
func TestPGAdminOutageFailsLoudly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	store, err := NewPG(ctx, unreachableDSN)
	if err != nil {
		t.Fatalf("NewPG with a syntactically valid dsn: %v", err)
	}
	defer store.Close()

	// The first exchange pays the dial timeout; the pool then knows the
	// database is down and the remaining calls fail fast. One context
	// bounds them all.
	if _, err := store.CreateUser(ctx, "Alice", RoleUser, "free"); err == nil {
		t.Fatal("CreateUser succeeded against a dead database")
	}
	if _, err := store.Users(ctx); err == nil {
		t.Fatal("Users succeeded against a dead database")
	}
	if err := store.SetUserLimits(ctx, "u1", LimitOverride{}); err == nil {
		t.Fatal("SetUserLimits succeeded against a dead database")
	}
	if _, err := store.CreateKey(ctx, "u1", "name"); err == nil {
		t.Fatal("CreateKey succeeded against a dead database")
	}
	if _, err := store.Keys(ctx, "u1"); err == nil {
		t.Fatal("Keys succeeded against a dead database")
	}
	if err := store.SetKeyLimits(ctx, "k1", LimitOverride{}); err == nil {
		t.Fatal("SetKeyLimits succeeded against a dead database")
	}
	if err := store.SetKeyStatus(ctx, "k1", false); err == nil {
		t.Fatal("SetKeyStatus succeeded against a dead database")
	}
}
