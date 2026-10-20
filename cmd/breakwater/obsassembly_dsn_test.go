/**
 * @file obsassembly_dsn_test
 * @description The DSN the assessment record store runs against: the
 * explicit insights DSN, the identity database's DSN it falls back
 * to, and the store that opens against either.
 */
package main

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/daftpunkwav/breakwater/internal/config"
)

// applyDeploySchema applies the deploy schema to the DSN-gated
// database, statement by statement, after stripping the comment lines
// (a bare semicolon split would otherwise execute comment-only
// fragments). The schema is idempotent by construction.
func applyDeploySchema(t *testing.T, dsn string) {
	t.Helper()
	// nosemgrep: go_filesystem_rule-fileread -- path is a fixed literal inside the repository
	schema, err := os.ReadFile(filepath.Join("..", "..", "deploy", "schema.sql"))
	if err != nil {
		t.Fatalf("read deploy schema: %v", err)
	}
	var cleaned strings.Builder
	for _, line := range strings.Split(string(schema), "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		cleaned.WriteString(line)
		cleaned.WriteByte('\n')
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer func() { _ = conn.Close(ctx) }()
	for _, stmt := range strings.Split(cleaned.String(), ";") {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		// nosemgrep: go_sql_rule-concat-sqli -- statements come from the repository's own deploy/schema.sql
		if _, err := conn.Exec(ctx, stmt); err != nil {
			t.Fatalf("apply schema statement %q: %v", firstLine(stmt), err)
		}
	}
}

// TestInsightsDSNResolution pins which DSN the assessment record store
// runs against: the explicit insights DSN wins, an empty one falls
// back to the identity database's DSN, and neither leaves the store
// disabled. The fallback is invisible in a running gateway, so the
// rule is pinned here.
func TestInsightsDSNResolution(t *testing.T) {
	t.Parallel()
	const identityDSN = "postgres://breakwater:breakwater@127.0.0.1:5432/breakwater?sslmode=disable"
	const explicitDSN = "postgres://breakwater:breakwater@127.0.0.1:5432/insights?sslmode=disable"

	// Neither configured: the store stays disabled.
	if dsn, source := insightsDSN(config.Config{}); dsn != "" || source != "" {
		t.Fatalf("no DSN = %q/%q, want none", dsn, source)
	}

	// Only the identity DSN: it becomes the store's DSN.
	cfg := config.Config{}
	cfg.Postgres.DSN = identityDSN
	dsn, source := insightsDSN(cfg)
	if dsn != identityDSN || source != "BREAKWATER_POSTGRES_DSN" {
		t.Fatalf("identity fallback = %q/%q, want the identity DSN", dsn, source)
	}

	// Both configured: the explicit insights DSN wins.
	cfg.Obs.InsightsDSN = explicitDSN
	dsn, source = insightsDSN(cfg)
	if dsn != explicitDSN || source != "BREAKWATER_INSIGHTS_DSN" {
		t.Fatalf("explicit DSN = %q/%q, want the insights DSN", dsn, source)
	}
}

// TestNewInsightsOpensStoreThroughIdentityFallback: a deployment that
// configures only the identity database still opens the assessment
// record store against it. The store is opened eagerly, so an
// unreachable database surfaces as a failed assembly — a gateway that
// silently stopped measuring would be worse.
func TestNewInsightsOpensStoreThroughIdentityFallback(t *testing.T) {
	t.Parallel()
	cfg := config.Config{}
	// An unreachable but well-formed DSN: the attempt is what proves
	// the fallback reached the store.
	cfg.Postgres.DSN = "postgres://breakwater:breakwater@127.0.0.1:1/db"
	if _, err := newInsights(context.Background(), cfg, slog.New(slog.DiscardHandler)); err == nil {
		t.Fatal("an unreachable identity database must fail the assembly")
	}
}

// TestNewInsightsFallsBackToIdentityDSN (CI only): with a live
// PostgreSQL the fallback opens a real record store against the
// identity database — the store a deployment that never sets an
// insights DSN persists its monitoring records in.
func TestNewInsightsFallsBackToIdentityDSN(t *testing.T) {
	dsn := os.Getenv("BREAKWATER_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("integration: BREAKWATER_TEST_POSTGRES_DSN not set")
	}
	applyDeploySchema(t, dsn)

	cfg := config.Config{}
	cfg.Postgres.DSN = dsn
	recorder, err := newInsights(context.Background(), cfg, slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatalf("newInsights: %v", err)
	}
	defer recorder.close()
	if recorder.store == nil {
		t.Fatal("the identity DSN must open the assessment record store")
	}
}
