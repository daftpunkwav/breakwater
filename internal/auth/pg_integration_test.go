/**
 * @file pg_integration_test
 * @description Integration against a real PostgreSQL: schema and seed
 * applied, keys resolve. Skipped unless BREAKWATER_TEST_POSTGRES_DSN
 * is set (the CI workflow provides it with service containers).
 */
package auth

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func TestPGResolveIntegration(t *testing.T) {
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

	for _, file := range []string{"../../deploy/schema.sql", "../../deploy/seed.sql"} {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatalf("read %s: %v", file, err)
		}
		if _, err := conn.Exec(ctx, string(raw)); err != nil {
			t.Fatalf("apply %s: %v", file, err)
		}
	}

	store, err := NewPG(ctx, dsn)
	if err != nil {
		t.Fatalf("store: %v", err)
	}

	tenant, err := store.Resolve(ctx, "bw-local-t1")
	if err != nil {
		t.Fatalf("resolve seeded key: %v", err)
	}
	if tenant.ID != "local-1" || tenant.Tier.ID != "free" || tenant.Tier.RPM != 60 {
		t.Fatalf("resolved %+v, want local-1/free with rpm 60", tenant)
	}

	if _, err := store.Resolve(ctx, "never-seeded"); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("unknown key err = %v, want ErrUnauthorized", err)
	}
}
