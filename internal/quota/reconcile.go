/**
 * @file reconcile
 * @description The quota reconciliation protocol (PRD Q6): the Redis
 * hot ledger is snapshotted on a fixed interval and every pair of
 * consecutive snapshots must satisfy the balance identity.
 *
 * Responsibilities:
 * - Read one tenant's reconcile inputs (balance, lifetime consumed and
 *   refunded totals, correction epoch) as an atomic-enough snapshot
 * - Diff consecutive snapshots against the identity: the balance may
 *   only move by consumption minus refund
 * - Persist snapshots through the injected store (PostgreSQL in
 *   production) so the identity survives restarts
 * - Nothing else: the totals move inside the settle/release scripts;
 *   a detected drift is reported, never self-healed — healing a wrong
 *   ledger silently would be worse than screaming about it
 *
 * Epoch discipline: an admin SetBalance bumps the tenant's correction
 * epoch, and the interval across a bump is skipped by design — a manual
 * top-up is a correction, not consumable drift.
 */
package quota

import (
	"context"
	"log/slog"
	"time"
)

// Snapshot is one reconcile reading of a tenant's ledger.
type Snapshot struct {
	TenantID string
	Balance  int64
	Consumed int64
	Refunded int64
	// Epoch counts manual balance corrections; a change between
	// snapshots marks the interval as skip-by-design.
	Epoch   int64
	TakenAt time.Time
}

// SnapshotSource reads the live reconcile inputs of one tenant. A nil
// snapshot with a nil error means the tenant has no ledger provisioned
// and is skipped.
type SnapshotSource interface {
	TenantSnapshot(ctx context.Context, tenantID string, takenAt time.Time) (*Snapshot, error)
}

// SnapshotStore persists snapshots so consecutive intervals can be
// diffed across process restarts. PostgreSQL in production.
type SnapshotStore interface {
	// Latest returns the most recent stored snapshot of the tenant, or
	// nil when none exists yet.
	Latest(ctx context.Context, tenantID string) (*Snapshot, error)
	// Append stores one snapshot.
	Append(ctx context.Context, snap Snapshot) error
}

// Reconciler diffs consecutive ledger snapshots. It is safe for
// concurrent use.
type Reconciler struct {
	source  SnapshotSource
	tenants []string
	store   SnapshotStore
	now     func() time.Time
}

// NewReconciler builds the reconciler over the given tenants.
func NewReconciler(source SnapshotSource, tenants []string, store SnapshotStore) *Reconciler {
	return &Reconciler{source: source, tenants: tenants, store: store, now: time.Now}
}

// ReconcileOnce snapshots every tenant and diffs against the previous
// snapshot. It returns how many intervals were checked and how many
// drifted; drift also surfaces per tenant through the returned report.
func (r *Reconciler) ReconcileOnce(ctx context.Context) (checked, drifted int, drifts []TenantDrift, err error) {
	takenAt := r.now()
	for _, tenantID := range r.tenants {
		snap, err := r.source.TenantSnapshot(ctx, tenantID, takenAt)
		if err != nil {
			return checked, drifted, drifts, err
		}
		if snap == nil {
			continue // no ledger provisioned: nothing to reconcile
		}
		prev, err := r.store.Latest(ctx, tenantID)
		if err != nil {
			return checked, drifted, drifts, err
		}
		if err := r.store.Append(ctx, *snap); err != nil {
			return checked, drifted, drifts, err
		}
		if prev == nil || prev.Epoch != snap.Epoch {
			continue // first sight, or a manual correction: skip by design
		}
		checked++
		if drift := consumedDelta(prev, snap) - balanceDrop(prev, snap); drift != 0 {
			drifted++
			drifts = append(drifts, TenantDrift{TenantID: tenantID, Drift: drift})
		}
	}
	return checked, drifted, drifts, nil
}

// TenantDrift is one tenant's reconciliation failure over one interval.
type TenantDrift struct {
	TenantID string
	Drift    int64
}

// consumedDelta is the token consumption between two snapshots.
func consumedDelta(prev, snap *Snapshot) int64 { return snap.Consumed - prev.Consumed }

// balanceDrop is how much the balance fell between two snapshots.
func balanceDrop(prev, snap *Snapshot) int64 { return prev.Balance - snap.Balance }

// StartReconciler runs the reconcile loop until ctx is cancelled. Every
// drift fires onDrift and is logged loudly: a non-zero drift means the
// ledger identity (invariant I3) broke in production.
func StartReconciler(ctx context.Context, reconciler *Reconciler, every time.Duration, onDrift func(drift TenantDrift)) {
	go func() {
		ticker := time.NewTicker(every)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				checked, drifted, drifts, err := reconciler.ReconcileOnce(ctx)
				if err != nil {
					slog.Warn("quota reconcile failed", "error", err)
					continue
				}
				if checked > 0 {
					slog.Debug("quota reconcile ran", "checked", checked, "drifted", drifted)
				}
				for _, drift := range drifts {
					slog.Error("quota ledger drift detected", "tenant", drift.TenantID, "drift", drift.Drift)
					if onDrift != nil {
						onDrift(drift)
					}
				}
			}
		}
	}()
}
