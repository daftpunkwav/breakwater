/**
 * @file authstore_test
 * @description Identity source resolution: the three branches of
 * newAuthStore and what each of them wires up.
 */
package main

import (
	"context"
	"log/slog"
	"testing"
)

func TestNewAuthStoreBranches(t *testing.T) {
	t.Parallel()
	logger := slog.New(slog.DiscardHandler)

	// No identity: no store, no admin surface, no probe, nothing to close.
	identity, err := newAuthStore(context.Background(), testConfig("127.0.0.1:0"))
	if err != nil || identity.store != nil || identity.static != nil || identity.admin != nil || identity.ready != nil {
		t.Fatalf("no identity = %+v err = %v, want all nil", identity, err)
	}
	identity.close()

	// Broken identity JSON fails at assembly.
	bad := testConfig("127.0.0.1:0")
	bad.Identity = "{not json"
	if _, err := newAuthStore(context.Background(), bad); err == nil {
		t.Fatal("broken identity JSON must fail assembly")
	}

	// A syntactically valid DSN assembles the pg store lazily: the
	// readiness probe surfaces the (here unreachable) database, and the
	// administration port rides on the same store.
	pg := testConfig("127.0.0.1:0")
	pg.Postgres.DSN = "postgres://breakwater:breakwater@127.0.0.1:1/db"
	pgIdentity, err := newAuthStore(context.Background(), pg)
	if err != nil || pgIdentity.store == nil || pgIdentity.admin == nil || pgIdentity.ready == nil {
		t.Fatalf("pg branch: %v", err)
	}
	defer pgIdentity.close()
	if err := pgIdentity.ready(); err == nil {
		t.Fatal("probe against an unreachable database must fail")
	}
	_ = logger
}
