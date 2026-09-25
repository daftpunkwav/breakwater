/**
 * @file pgadmin_integration_test
 * @description The administration port against a real PostgreSQL:
 * user creation, the key cap, both override layers merging into live
 * resolution, and key disabling. Skipped unless
 * BREAKWATER_TEST_POSTGRES_DSN is set (CI provides it).
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

func TestPGAdminStoreIntegration(t *testing.T) {
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
	for _, file := range []string{"../../deploy/schema.sql"} {
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

	userID, err := store.CreateUser(ctx, "Alice", RoleUser, "free")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	if _, err := store.CreateUser(ctx, "Bob", RoleUser, "no-such-tier"); !errors.Is(err, ErrUnknownTier) {
		t.Fatalf("unknown tier err = %v, want ErrUnknownTier", err)
	}

	// User-level limits: deny a model, cap concurrency.
	deny := LimitOverride{DeniedModels: []string{"secret-model"}, Concurrency: ptrInt64(2)}
	if err := store.SetUserLimits(ctx, userID, deny); err != nil {
		t.Fatalf("set user limits: %v", err)
	}
	if err := store.SetUserLimits(ctx, "u_ghost", deny); !errors.Is(err, ErrUnknownUser) {
		t.Fatalf("ghost user err = %v, want ErrUnknownUser", err)
	}

	// Key issuance up to the cap, then the cap bites.
	first, err := store.CreateKey(ctx, userID, "laptop")
	if err != nil {
		t.Fatalf("create key: %v", err)
	}
	if first.Raw == "" || len(first.Raw) < 20 {
		t.Fatalf("issued raw key = %q, want a real secret", first.Raw)
	}
	// A key for a nonexistent user fails at the foreign key, mapped to
	// the sentinel — not at the count query.
	if _, err := store.CreateKey(ctx, "u_ghost", "ghost"); !errors.Is(err, ErrUnknownUser) {
		t.Fatalf("ghost-user key err = %v, want ErrUnknownUser", err)
	}
	for i := 1; i < MaxKeysPerUser; i++ {
		if _, err := store.CreateKey(ctx, userID, "extra"); err != nil {
			t.Fatalf("key %d: %v", i+1, err)
		}
	}
	if _, err := store.CreateKey(ctx, userID, "one too many"); !errors.Is(err, ErrTooManyKeys) {
		t.Fatalf("cap err = %v, want ErrTooManyKeys", err)
	}

	// The user listing reflects what the surface created.
	users, err := store.Users(ctx)
	if err != nil || len(users) == 0 {
		t.Fatalf("users = %v err = %v, want at least the seeded ones", users, err)
	}

	// Key-level limits + disable.
	keyID := first.ID
	rpm := int64(7)
	if err := store.SetKeyLimits(ctx, keyID, LimitOverride{RPM: &rpm}); err != nil {
		t.Fatalf("set key limits: %v", err)
	}
	if err := store.SetKeyLimits(ctx, "k_ghost", LimitOverride{}); !errors.Is(err, ErrUnknownKey) {
		t.Fatalf("ghost key err = %v, want ErrUnknownKey", err)
	}
	if err := store.SetKeyStatus(ctx, keyID, false); err != nil {
		t.Fatalf("disable key: %v", err)
	}

	// The listing reflects the cap and the disabled status.
	keys, err := store.Keys(ctx, userID)
	if err != nil || len(keys) != MaxKeysPerUser {
		t.Fatalf("keys = %v err = %v, want %d", keys, err, MaxKeysPerUser)
	}
	for _, k := range keys {
		if k.ID == keyID && k.Active {
			t.Fatal("the disabled key still lists as active")
		}
	}

	// Fresh user, active key: tier ∪ user-deny ∪ key-rpm must merge.
	second, err := store.CreateUser(ctx, "Mallory", RoleAdmin, "free")
	if err != nil {
		t.Fatalf("create second user: %v", err)
	}
	if err := store.SetUserLimits(ctx, second, LimitOverride{DeniedModels: []string{"secret-model"}}); err != nil {
		t.Fatalf("set second user limits: %v", err)
	}
	issued, err := store.CreateKey(ctx, second, "probe")
	if err != nil {
		t.Fatalf("issue probe key: %v", err)
	}
	if err := store.SetKeyLimits(ctx, issued.ID, LimitOverride{RPM: &rpm}); err != nil {
		t.Fatalf("set probe key limits: %v", err)
	}
	tenant, err := store.Resolve(ctx, issued.Raw)
	if err != nil {
		t.Fatalf("resolve issued key: %v", err)
	}
	if tenant.Role != RoleAdmin || tenant.Tier.RPM != 7 {
		t.Fatalf("resolved role=%s rpm=%d, want admin with the key-layer rpm 7", tenant.Role, tenant.Tier.RPM)
	}
	if tenant.Tier.AllowsModel("secret-model") {
		t.Fatal("the user-level deny must hold in the resolved snapshot")
	}
	if !tenant.Tier.AllowsModel("any-model") {
		t.Fatal("unlisted models must stay allowed by the tier wildcard")
	}
}

func ptrInt64(v int64) *int64 { return &v }
