/**
 * @file pgsnapshot_offline_test
 * @description The PostgreSQL snapshot store's failure paths, without a
 * database: an unparseable DSN fails at assembly, and an unreachable
 * database surfaces wrapped read/write errors instead of fabricated
 * snapshots. The happy path lives in the env-gated integration test.
 */
package quota

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestPGSnapshotStoreRejectsUnparseableDSN(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := NewPGSnapshotStore(ctx, "not a parseable dsn"); err == nil ||
		!strings.Contains(err.Error(), "connect snapshot database") {
		t.Fatalf("err = %v, want the wrapped connection error", err)
	}
}

// TestPGSnapshotStoreErrorRedactsDSNPassword pins the startup-error
// contract the three pg constructors share (quota snapshots, identity,
// insights): a parse failure surfaces the DSN, so the driver's password
// redaction must hold — the error reaches the operator's console and
// logs, where the raw password must never appear.
func TestPGSnapshotStoreErrorRedactsDSNPassword(t *testing.T) {
	t.Parallel()
	for _, dsn := range []string{
		"postgres://user:secretpw@127.0.0.1:5432/quota?sslmode=bogus",
		"host=127.0.0.1 password=secretpw user=u port=notanum",
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err := NewPGSnapshotStore(ctx, dsn)
		cancel()
		if err == nil {
			t.Fatalf("dsn %q: want a parse rejection", dsn)
		}
		if msg := err.Error(); strings.Contains(msg, "secretpw") {
			t.Fatalf("error message carries the DSN password: %q", msg)
		}
	}
}

func TestPGSnapshotStoreSurfacesUnreachableDatabase(t *testing.T) {
	t.Parallel()
	// Port 1 on loopback refuses connections immediately; pgx pools are
	// lazy, so assembly succeeds and the first query fails.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := NewPGSnapshotStore(ctx, "postgres://quota:secret@127.0.0.1:1/quota?connect_timeout=2")
	if err != nil {
		t.Fatalf("lazy pool assembly: %v", err)
	}
	t.Cleanup(store.Close)

	if _, err := store.Latest(ctx, "t"); err == nil ||
		!strings.Contains(err.Error(), "latest snapshot") {
		t.Fatalf("latest err = %v, want the wrapped read failure", err)
	}
	if err := store.Append(ctx, Snapshot{TenantID: "t"}); err == nil ||
		!strings.Contains(err.Error(), "append snapshot") {
		t.Fatalf("append err = %v, want the wrapped write failure", err)
	}
}
