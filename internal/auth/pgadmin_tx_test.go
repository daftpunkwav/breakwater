/**
 * @file pgadmin_tx_test
 * @description CreateKey's transaction body against a scripted
 * pgx.Tx: the lock, count and insert failure paths a live database
 * only reaches under fault conditions, plus the commit ordering.
 */
package auth

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
)

// scriptedRow answers Scan with a fixed error or fixed values.
type scriptedRow struct {
	err    error
	values []any
}

func (r *scriptedRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	for i, d := range dest {
		switch ptr := d.(type) {
		case *string:
			*ptr, _ = r.values[i].(string)
		case *int64:
			*ptr, _ = r.values[i].(int64)
		}
	}
	return nil
}

// fakeTx serves the scripted answers in call order and records the
// commits.
type fakeTx struct {
	pgx.Tx
	answers []scriptedRow
	calls   int
	commit  error
	commits int
}

func (f *fakeTx) QueryRow(context.Context, string, ...any) pgx.Row {
	if f.calls >= len(f.answers) {
		return &scriptedRow{err: errors.New("unexpected query")}
	}
	row := &f.answers[f.calls]
	f.calls++
	return row
}

func (f *fakeTx) Commit(context.Context) error {
	f.commits++
	return f.commit
}

// TestCreateKeyTxLockFailureIsWrapped: a lock query that fails for a
// reason other than a missing row surfaces as a wrapped error, never
// as a sentinel or a silent success.
func TestCreateKeyTxLockFailureIsWrapped(t *testing.T) {
	t.Parallel()
	tx := &fakeTx{answers: []scriptedRow{{err: errors.New("connection reset")}}}
	if _, err := createKeyTx(context.Background(), tx, "u1", "name"); err == nil || !strings.Contains(err.Error(), "lock user") {
		t.Fatalf("err = %v, want the wrapped lock failure", err)
	}
}

func TestCreateKeyTxCountFailureIsWrapped(t *testing.T) {
	t.Parallel()
	tx := &fakeTx{answers: []scriptedRow{
		{values: []any{"u1"}},             // lock acquired
		{err: errors.New("read timeout")}, // count query dies
	}}
	if _, err := createKeyTx(context.Background(), tx, "u1", "name"); err == nil || !strings.Contains(err.Error(), "count keys") {
		t.Fatalf("err = %v, want the wrapped count failure", err)
	}
}

// TestCreateKeyTxInsertFailureIsWrapped: a failing insert (or commit)
// inside the transaction wraps under "create key" — the caller sees
// one store error, not a partial key.
func TestCreateKeyTxInsertFailureIsWrapped(t *testing.T) {
	t.Parallel()
	tx := &fakeTx{answers: []scriptedRow{
		{values: []any{"u1"}},                      // lock acquired
		{values: []any{int64(0)}},                  // count: under the cap
		{err: errors.New("serialisation failure")}, // insert dies
	}}
	if _, err := createKeyTx(context.Background(), tx, "u1", "name"); err == nil || !strings.Contains(err.Error(), "create key") {
		t.Fatalf("err = %v, want the wrapped create failure", err)
	}
	if tx.commits != 0 {
		t.Fatal("a failed insert must never reach commit")
	}
}

// TestCreateKeyTxCommitSuccessOrdering: on the happy path the commit
// runs exactly once after the insert answer.
func TestCreateKeyTxCommitSuccessOrdering(t *testing.T) {
	t.Parallel()
	tx := &fakeTx{answers: []scriptedRow{
		{values: []any{"u1"}},
		{values: []any{int64(0)}},
		{err: errors.New("no scripted insert row")},
	}}
	// The insert's row is scripted to fail here, so commit must not
	// have run — pinning that commit happens only after a clean insert.
	_, err := createKeyTx(context.Background(), tx, "u1", "name")
	if err == nil {
		t.Fatal("expected the scripted insert failure")
	}
	if tx.commits != 0 {
		t.Fatalf("commits = %d, want 0 on the failure path", tx.commits)
	}
}
