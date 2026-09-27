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
	store, static, admin, closeFn, probe, err := newAuthStore(context.Background(), testConfig("127.0.0.1:0"))
	if err != nil || store != nil || static != nil || admin != nil || probe != nil {
		t.Fatalf("no identity = %+v err = %v, want all nil", store, err)
	}
	closeFn()

	// Broken identity JSON fails at assembly.
	bad := testConfig("127.0.0.1:0")
	bad.Identity = "{not json"
	if _, _, _, _, _, err := newAuthStore(context.Background(), bad); err == nil {
		t.Fatal("broken identity JSON must fail assembly")
	}

	// A syntactically valid DSN assembles the pg store lazily: the
	// readiness probe surfaces the (here unreachable) database, and the
	// administration port rides on the same store.
	pg := testConfig("127.0.0.1:0")
	pg.Postgres.DSN = "postgres://breakwater:breakwater@127.0.0.1:1/db"
	pgStore, _, pgAdmin, pgClose, pgProbe, err := newAuthStore(context.Background(), pg)
	if err != nil || pgStore == nil || pgAdmin == nil || pgProbe == nil {
		t.Fatalf("pg branch: %v", err)
	}
	defer pgClose()
	if err := pgProbe(); err == nil {
		t.Fatal("probe against an unreachable database must fail")
	}
	_ = logger
}
