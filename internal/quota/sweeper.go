/**
 * @file sweeper
 * @description The background reclaim loop for abandoned quota leases.
 *
 * Responsibilities:
 * - Periodically reclaim RESERVED leases whose holder vanished before
 *   settling (process crash between reserve and settle, invariant I9)
 * - Surface every reclamation through a counter hook so the operations
 *   story stays honest
 * - Nothing else: the reclaim transition itself belongs to the ledger
 *
 * One sweeper goroutine per process; reclamation is idempotent, so
 * even a multi-process future stays correct.
 */
package quota

import (
	"context"
	"log/slog"
	"time"
)

// SweepTarget is the reclaim surface the sweeper drives.
type SweepTarget interface {
	SweepOnce(ctx context.Context, now time.Time, limit int) (int, error)
}

// StartSweeper runs the reclaim loop until ctx is cancelled. The first
// sweep happens after the first tick, not before — startup must not
// wait on reclamation. onExpired observes each reclaimed batch; nil
// disables observation.
func StartSweeper(ctx context.Context, target SweepTarget, every time.Duration, onExpired func(count int)) {
	go func() {
		ticker := time.NewTicker(every)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				expired, err := target.SweepOnce(ctx, time.Now(), 1000)
				if err != nil {
					slog.Warn("quota sweep failed", "error", err)
					continue
				}
				if expired > 0 {
					slog.Warn("expired quota reservations reclaimed", "count", expired)
					if onExpired != nil {
						onExpired(expired)
					}
				}
			}
		}
	}()
}
