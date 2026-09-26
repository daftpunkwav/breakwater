/**
 * @file pg_row_mapping_test
 * @description The identity row mapping seam of the pg store: one
 * query row to the tenant snapshot, with the ErrNoRows and scan
 * failure mappings — unit-tested without a database.
 */
package auth

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// fakeRow is a scripted single-row query result.
type fakeRow struct {
	values []any
	err    error
}

// Scan implements rowScanner: it either reports the scripted error or
// copies the scripted values into the supported destinations.
func (r *fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != len(r.values) {
		return fmt.Errorf("fakeRow: %d destinations, %d values", len(dest), len(r.values))
	}
	for i, d := range dest {
		switch ptr := d.(type) {
		case *string:
			v, ok := r.values[i].(string)
			if !ok {
				return fmt.Errorf("fakeRow: value %d is %T, want string", i, r.values[i])
			}
			*ptr = v
		case *Role:
			v, ok := r.values[i].(string)
			if !ok {
				return fmt.Errorf("fakeRow: value %d is %T, want string", i, r.values[i])
			}
			*ptr = Role(v)
		case *int64:
			v, ok := r.values[i].(int64)
			if !ok {
				return fmt.Errorf("fakeRow: value %d is %T, want int64", i, r.values[i])
			}
			*ptr = v
		case *[]string:
			v, ok := r.values[i].([]string)
			if !ok {
				return fmt.Errorf("fakeRow: value %d is %T, want []string", i, r.values[i])
			}
			*ptr = v
		case *[]byte:
			switch v := r.values[i].(type) {
			case []byte:
				*ptr = v
			case nil:
				*ptr = nil
			default:
				return fmt.Errorf("fakeRow: value %d is %T, want bytes", i, r.values[i])
			}
		default:
			return fmt.Errorf("fakeRow: unsupported scan destination %T", d)
		}
	}
	return nil
}

// fullRowValues is a healthy identity row: the tenant, its role, the
// tier template and both override layers (user-level then key-level,
// as JSON documents).
var fullRowValues = []any{
	"tenant-1", "Acme", "user", "key-9",
	"tier-pro", int64(600), int64(90_000),
	int64(4096), int64(50_000_000),
	[]string{"m1", "m2"},
	[]byte(`{"denied_models":["m2"],"monthly_quota":1000}`),
	[]byte(`{"rpm":30}`),
}

// TestResolveTenantRowMapsFullRow: the tenant snapshot is built from
// the joined row with both override layers merged in — the key's rpm
// wins over the tier's, the user's quota wins, the user's deny removes
// a model the tier allows.
func TestResolveTenantRowMapsFullRow(t *testing.T) {
	t.Parallel()

	tenant, err := resolveTenantRow(&fakeRow{values: fullRowValues})
	if err != nil {
		t.Fatalf("resolveTenantRow: %v", err)
	}
	if tenant.ID != "tenant-1" || tenant.Name != "Acme" {
		t.Fatalf("tenant identity = %s/%s", tenant.ID, tenant.Name)
	}
	if tenant.Role != RoleUser {
		t.Fatalf("role = %q, want user", tenant.Role)
	}
	if tenant.KeyID != "key-9" {
		t.Fatalf("key id = %q, want key-9", tenant.KeyID)
	}
	tier := tenant.Tier
	if tier.ID != "tier-pro" || tier.MaxTokens != 4096 {
		t.Fatalf("tier snapshot = %+v, want the row's tier columns", tier)
	}
	if tier.RPM != 30 {
		t.Fatalf("rpm = %d, want the key override's 30", tier.RPM)
	}
	if tier.MonthlyQuota != 1000 {
		t.Fatalf("quota = %d, want the user override's 1000", tier.MonthlyQuota)
	}
	if !tier.AllowsModel("m1") {
		t.Fatal("m1 must survive the merge")
	}
	if tier.AllowsModel("m2") {
		t.Fatal("the user-level deny must remove m2")
	}
}

// TestResolveTenantRowMappings: the error mapping contract — a missing
// row is the definitive ErrUnauthorized, every other scan failure is a
// wrapped transient error that is not ErrUnauthorized.
func TestResolveTenantRowMappings(t *testing.T) {
	scanErr := errors.New("cannot scan uuid into string")

	cases := []struct {
		name        string
		row         *fakeRow
		wantErr     error  // errors.Is target of the returned error
		wantErrText string // substring of the returned error, when set
	}{
		{"unknown key", &fakeRow{err: pgx.ErrNoRows}, ErrUnauthorized, ""},
		{"scan failure wraps the cause", &fakeRow{err: scanErr}, scanErr, "resolve key"},
		{"target mismatch wraps the scan error", &fakeRow{values: []any{"t", "n", "user", "tier", "not-an-int"}}, nil, "fakeRow"},
		{"broken user override fails the resolution", &fakeRow{values: append(append([]any{}, fullRowValues[:10]...), []byte("{broken"), []byte(nil))}, nil, "overrides"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tenant, err := resolveTenantRow(tc.row)
			if err == nil {
				t.Fatalf("resolveTenantRow = (%+v, nil), want an error", tenant)
			}
			if !reflect.DeepEqual(tenant, Tenant{}) {
				t.Fatalf("tenant = %+v alongside error %v, want the zero tenant", tenant, err)
			}
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want errors.Is %v", err, tc.wantErr)
			}
			if tc.wantErrText != "" && !strings.Contains(err.Error(), tc.wantErrText) {
				t.Fatalf("err = %q, want it to contain %q", err, tc.wantErrText)
			}
			// An unknown key alone must be distinguishable from a store
			// outage: only ErrNoRows produces ErrUnauthorized.
			if errors.Is(err, ErrUnauthorized) && !errors.Is(tc.row.err, pgx.ErrNoRows) {
				t.Fatalf("err = %v, a scan failure must not surface as ErrUnauthorized", err)
			}
		})
	}
}

// TestPGConnectRejectsInvalidDSN: connection assembly fails at startup
// for an unparseable DSN.
func TestPGConnectRejectsInvalidDSN(t *testing.T) {
	t.Parallel()

	if _, err := NewPG(context.Background(), "not a valid dsn"); err == nil {
		t.Fatal("NewPG accepted an invalid dsn")
	}
}
