package flowlifecycle

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"
)

// AutoDeleter performs the manual raw-deletion steps for a published policy
// with AutoDelete, as a system actor: it approves a reconciled day whose
// deletion evidence is complete, publishes the delete barrier, and schedules
// the delete job once every required Flow worker has installed the barrier.
// Every gate of the manual path still applies; the job itself re-verifies the
// frozen evidence before dropping the partition.
type AutoDeleter struct {
	Store    *Store
	Evidence RawDayEvidenceReader
	// Actor is the users.id recorded on approvals, barriers and receipts.
	Actor    string
	Interval time.Duration
	MaxDays  int
	Logf     func(string, ...any)
}

func (deleter *AutoDeleter) Run(ctx context.Context) {
	if deleter == nil || deleter.Store == nil || deleter.Evidence == nil || strings.TrimSpace(deleter.Actor) == "" ||
		deleter.Interval <= 0 || deleter.MaxDays < 1 {
		return
	}
	run := func(now time.Time) {
		approved, scheduled, err := deleter.ScanOnce(ctx, now)
		if deleter.Logf != nil && (err != nil || approved > 0 || scheduled > 0) {
			deleter.Logf("Flow raw auto-delete approved=%d scheduled=%d err=%v", approved, scheduled, err)
		}
	}
	run(time.Now())
	ticker := time.NewTicker(deleter.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			run(now)
		}
	}
}

// ScanOnce advances every candidate day by as many steps as its evidence and
// the delete barrier allow. A day that is not ready yet is left for a later
// scan; the scan stops at the first store error.
func (deleter *AutoDeleter) ScanOnce(ctx context.Context, now time.Time) (approved, scheduled int, err error) {
	policy, err := deleter.Store.GetPublishedPolicy(ctx)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, err
	}
	if !policy.AutoDelete || !policy.RawDeleteEnabled {
		return 0, 0, nil
	}
	now = now.UTC()
	candidates, err := deleter.Store.ListAutoDeleteCandidates(ctx, policy.Version, now, deleter.MaxDays)
	if err != nil {
		return 0, 0, err
	}
	for _, state := range candidates {
		approvalID := state.DeleteApprovalID
		var approvalVersion uint64
		if state.State == PartitionReconciled {
			readiness, err := deleter.Store.RawDayDeleteReadiness(ctx, state.SourceDate, now, deleter.Evidence)
			if err != nil {
				return approved, scheduled, err
			}
			if !readiness.EvidenceReady {
				if deleter.Logf != nil {
					deleter.Logf("Flow raw auto-delete %s waiting: %s", state.SourceDate.Format(time.DateOnly), strings.Join(readiness.Blockers, ","))
				}
				continue
			}
			approval, _, err := deleter.Store.ApproveRawDayDelete(ctx, readiness, readiness.PartitionVersion, deleter.Actor, now)
			if err != nil {
				return approved, scheduled, err
			}
			approved++
			approvalID, approvalVersion = approval.ID, approval.RowVersion
		} else {
			approval, err := deleter.Store.GetDeletionApproval(ctx, approvalID)
			if err != nil {
				return approved, scheduled, err
			}
			approvalVersion = approval.RowVersion
		}
		barrier, err := deleter.Store.EnsureRawDeleteBarrierForApproval(ctx, approvalID, approvalVersion, deleter.Actor, now)
		if err != nil {
			return approved, scheduled, err
		}
		if !barrier.Ready {
			continue
		}
		if _, _, _, err := deleter.Store.ScheduleRawDayDelete(ctx, approvalID, approvalVersion, deleter.Actor, now); err != nil {
			return approved, scheduled, err
		}
		scheduled++
	}
	return approved, scheduled, nil
}

// ListAutoDeleteCandidates returns, oldest first, the published revision's
// days that auto-delete can advance: reconciled days past their delete-eligible
// instant, and approved days whose delete job has not been scheduled.
func (store *Store) ListAutoDeleteCandidates(ctx context.Context, policyVersion uint64, now time.Time, limit int) ([]PartitionState, error) {
	if store == nil || store.db == nil || policyVersion == 0 || now.IsZero() || limit < 1 || limit > 366 {
		return nil, ErrInvalidPolicy
	}
	rows, err := store.db.QueryContext(ctx, `SELECT `+partitionColumns+` FROM flow_retention_partition_states
		WHERE policy_version=? AND delete_job_id IS NULL AND raw_deleted_at IS NULL
		  AND ((state='reconciled' AND delete_eligible_at IS NOT NULL AND delete_eligible_at<=?)
		    OR (state='delete_eligible' AND delete_approval_id IS NOT NULL))
		ORDER BY source_date ASC LIMIT ?`, policyVersion, now.UTC().Truncate(time.Millisecond), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]PartitionState, 0)
	for rows.Next() {
		item, err := scanPartition(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}
