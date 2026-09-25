/**
 * @file pgsnapshot
 * @description The PostgreSQL snapshot store of the reconcile protocol:
 * the durable side of the ledger identity check.
 *
 * Responsibilities:
 * - Persist one ledger snapshot per tenant and interval
 * - Serve the latest stored snapshot of a tenant for diffing
 * - Nothing else: reading the live ledger belongs to the source, drift
 *   reporting to the reconciler
 */
package quota

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PGSnapshots stores reconcile snapshots in PostgreSQL. It is safe for
// concurrent use.
type PGSnapshots struct {
	pool *pgxpool.Pool
}

// NewPGSnapshots connects the snapshot store; failures surface at
// assembly.
func NewPGSnapshots(ctx context.Context, dsn string) (*PGSnapshots, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("quota: connect snapshot database: %w", err)
	}
	return &PGSnapshots{pool: pool}, nil
}

// Close releases the pool; shutdown path only.
func (s *PGSnapshots) Close() {
	s.pool.Close()
}

// Latest implements SnapshotStore.
func (s *PGSnapshots) Latest(ctx context.Context, tenantID string) (*Snapshot, error) {
	row := s.pool.QueryRow(ctx, `
		SELECT balance, consumed, refunded, epoch, taken_at
		FROM quota_snapshots
		WHERE tenant_id = $1
		ORDER BY taken_at DESC, id DESC
		LIMIT 1`, tenantID)

	var snap Snapshot
	var takenAt time.Time
	err := row.Scan(&snap.Balance, &snap.Consumed, &snap.Refunded, &snap.Epoch, &takenAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("quota: latest snapshot: %w", err)
	}
	snap.TenantID = tenantID
	snap.TakenAt = takenAt
	return &snap, nil
}

// Append implements SnapshotStore.
func (s *PGSnapshots) Append(ctx context.Context, snap Snapshot) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO quota_snapshots (tenant_id, balance, consumed, refunded, epoch, taken_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		snap.TenantID, snap.Balance, snap.Consumed, snap.Refunded, snap.Epoch, snap.TakenAt)
	if err != nil {
		return fmt.Errorf("quota: append snapshot: %w", err)
	}
	return nil
}
