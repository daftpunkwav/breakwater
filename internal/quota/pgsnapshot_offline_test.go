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

func TestPGSnapshotsRejectsUnparseableDSN(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := NewPGSnapshots(ctx, "not a parseable dsn"); err == nil ||
		!strings.Contains(err.Error(), "connect snapshot database") {
		t.Fatalf("err = %v, want the wrapped connection error", err)
	}
}

func TestPGSnapshotsSurfacesUnreachableDatabase(t *testing.T) {
	t.Parallel()
	// Port 1 on loopback refuses connections immediately; pgx pools are
	// lazy, so assembly succeeds and the first query fails.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	store, err := NewPGSnapshots(ctx, "postgres://quota:secret@127.0.0.1:1/quota?connect_timeout=2")
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
