/**
 * @file pgsnapshot_integration_test
 * @description Integration against a real PostgreSQL: snapshots append
 * and the latest read returns them in order. Skipped unless
 * BREAKWATER_TEST_POSTGRES_DSN is set (the CI workflow provides it).
 */
package quota

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestPGSnapshotStoreIntegration(t *testing.T) {
	dsn := os.Getenv("BREAKWATER_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("integration: BREAKWATER_TEST_POSTGRES_DSN not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	if _, err := conn.Exec(ctx, `CREATE TABLE IF NOT EXISTS quota_snapshots (
		id         BIGSERIAL PRIMARY KEY,
		tenant_id  TEXT NOT NULL,
		balance    BIGINT NOT NULL,
		consumed   BIGINT NOT NULL,
		refunded   BIGINT NOT NULL,
		epoch      BIGINT NOT NULL DEFAULT 0,
		taken_at   TIMESTAMPTZ NOT NULL DEFAULT now())`); err != nil {
		t.Fatalf("apply snapshot table: %v", err)
	}

	store, err := NewPGSnapshots(ctx, dsn)
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	if latest, err := store.Latest(ctx, "reconcile-int"); err != nil || latest != nil {
		t.Fatalf("latest of unknown tenant = %+v err = %v, want nil/nil", latest, err)
	}

	taken := time.Now().UTC().Truncate(time.Microsecond)
	snap := Snapshot{
		TenantID: "reconcile-int",
		Balance:  900,
		Consumed: 70,
		Refunded: 30,
		Epoch:    2,
		TakenAt:  taken,
	}
	if err := store.Append(ctx, snap); err != nil {
		t.Fatalf("append: %v", err)
	}
	latest, err := store.Latest(ctx, "reconcile-int")
	if err != nil {
		t.Fatalf("latest: %v", err)
	}
	if latest == nil || latest.Balance != 900 || latest.Consumed != 70 ||
		latest.Refunded != 30 || latest.Epoch != 2 {
		t.Fatalf("latest = %+v", latest)
	}
}
