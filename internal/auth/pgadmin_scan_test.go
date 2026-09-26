/**
 * @file pgadmin_scan_test
 * @description The admin listing row mappings over a scripted rows
 * scanner: view folding, the active flag, corrupted overrides and the
 * error propagation of every scan path.
 */
package auth

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// adminRows scripts a rows result: tuples advance with Next, Scan
// fills the supported destinations, and the trailing error propagates.
type adminRows struct {
	tuples [][]any
	i      int
	err    error
}

func (r *adminRows) Next() bool { return r.i < len(r.tuples) }

func (r *adminRows) Scan(dest ...any) error {
	tuple := r.tuples[r.i]
	r.i++
	for i, d := range dest {
		if i >= len(tuple) {
			continue
		}
		v := tuple[i]
		switch ptr := d.(type) {
		case *string:
			s, ok := v.(string)
			if !ok {
				return errors.New("type mismatch")
			}
			*ptr = s
		case *Role:
			s, ok := v.(string)
			if !ok {
				return errors.New("type mismatch")
			}
			*ptr = Role(s)
		case *[]byte:
			switch raw := v.(type) {
			case []byte:
				*ptr = raw
			case nil:
				*ptr = nil
			default:
				return errors.New("type mismatch")
			}
		case *time.Time:
			ts, ok := v.(time.Time)
			if !ok {
				return errors.New("type mismatch")
			}
			*ptr = ts
		}
	}
	return nil
}

func (r *adminRows) Err() error { return r.err }

// TestScanUsersFoldsViews: the view carries the identity, the role and
// the parsed overrides document.
func TestScanUsersFoldsViews(t *testing.T) {
	t.Parallel()
	when := time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC)
	rows := &adminRows{tuples: [][]any{
		{"u1", "Alice", "admin", "free", []byte(`{"rpm":10}`), when},
		{"u2", "Bob", "user", "free", []byte(nil), when},
	}}

	users, err := scanUsers(rows)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(users) != 2 || users[0].ID != "u1" || users[0].Role != RoleAdmin {
		t.Fatalf("users = %+v", users)
	}
	if users[0].Overrides.RPM == nil || *users[0].Overrides.RPM != 10 {
		t.Fatalf("overrides = %+v, want rpm 10", users[0].Overrides)
	}
	// A nil document parses as no override.
	if !users[1].Overrides.Empty() {
		t.Fatalf("nil overrides = %+v, want empty", users[1].Overrides)
	}
}

func TestScanUsersRejectsCorruptedOverrides(t *testing.T) {
	t.Parallel()
	rows := &adminRows{tuples: [][]any{
		{"u1", "Alice", "user", "free", []byte("{broken"), time.Time{}},
	}}
	if _, err := scanUsers(rows); err == nil || !strings.Contains(err.Error(), "overrides") {
		t.Fatalf("err = %v, want the overrides corruption to fail the listing", err)
	}
}

func TestScanUsersPropagatesErrors(t *testing.T) {
	t.Parallel()
	boom := errors.New("connection reset")

	// A failing scan inside the loop.
	rows := &adminRows{tuples: [][]any{{"u1", 42, "user", "free", []byte(nil), time.Time{}}}}
	if _, err := scanUsers(rows); err == nil || !strings.Contains(err.Error(), "scan user") {
		t.Fatalf("err = %v, want the wrapped scan failure", err)
	}
	// The trailing rows.Err.
	rows = &adminRows{tuples: [][]any{{"u1", "A", "user", "free", []byte(nil), time.Time{}}}, err: boom}
	if _, err := scanUsers(rows); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the trailing rows error", err)
	}
}

// TestScanKeysFoldsViews: the active flag decodes from the status
// column and the overrides parse.
func TestScanKeysFoldsViews(t *testing.T) {
	t.Parallel()
	when := time.Date(2026, 9, 26, 0, 0, 0, 0, time.UTC)
	rows := &adminRows{tuples: [][]any{
		{"k1", "laptop", "active", []byte(`{"rpm":5}`), when},
		{"k2", "phone", "disabled", []byte(nil), when},
	}}

	keys, err := scanKeys(rows)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	if len(keys) != 2 || !keys[0].Active || keys[1].Active {
		t.Fatalf("keys = %+v, want k1 active and k2 disabled", keys)
	}
	if keys[0].Overrides.RPM == nil || *keys[0].Overrides.RPM != 5 {
		t.Fatalf("overrides = %+v, want rpm 5", keys[0].Overrides)
	}
}

func TestScanKeysRejectsCorruptedOverrides(t *testing.T) {
	t.Parallel()
	rows := &adminRows{tuples: [][]any{
		{"k1", "laptop", "active", []byte("{broken"), time.Time{}},
	}}
	if _, err := scanKeys(rows); err == nil || !strings.Contains(err.Error(), "overrides") {
		t.Fatalf("err = %v, want the overrides corruption to fail the listing", err)
	}
}

func TestScanKeysPropagatesErrors(t *testing.T) {
	t.Parallel()
	boom := errors.New("connection reset")

	rows := &adminRows{tuples: [][]any{{"k1", 42, "active", []byte(nil), time.Time{}}}}
	if _, err := scanKeys(rows); err == nil || !strings.Contains(err.Error(), "scan key") {
		t.Fatalf("err = %v, want the wrapped scan failure", err)
	}
	rows = &adminRows{tuples: [][]any{{"k1", "n", "active", []byte(nil), time.Time{}}}, err: boom}
	if _, err := scanKeys(rows); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the trailing rows error", err)
	}
}
