// SPDX-FileCopyrightText: 2026 Watchdog contributors
// SPDX-License-Identifier: AGPL-3.0-only

package flowlifecycle

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/flowch"
	"github.com/cloudcache/watchdog/internal/opjob"
)

const (
	ArchiveDeleteJobType       = "flow_storage_archive_delete"
	archiveDeletePayloadSchema = 1
)

type ArchiveDeletePayload struct {
	ApprovalID    string `json:"approval_id"`
	MonthStart    string `json:"month_start"`
	Generation    uint64 `json:"generation"`
	ReceiptSchema uint16 `json:"receipt_schema"`
}

type ArchiveDeleteExecution struct {
	Approval DeletionApproval
	Receipt  DeletionReceipt
	Policy   Policy
}

func NewArchiveDeleteOperationJob(approval DeletionApproval, actor string) (opjob.Job, error) {
	month, end, err := archiveMonth(approval.PartitionStart)
	if err != nil || approval.ID == "" || approval.Status != "approved" || approval.StorageKind != "archive" ||
		approval.PartitionGranularity != "month" || !approval.PartitionEnd.Equal(end) || approval.Generation == 0 ||
		approval.Source != approval.Archive || approval.PhysicalRecords == 0 || len(approval.KafkaCoverage) != 0 || strings.TrimSpace(actor) == "" {
		return opjob.Job{}, ErrInvalidDeletionApproval
	}
	payload, err := opjob.EncodePayload(archiveDeletePayloadSchema, ArchiveDeletePayload{
		ApprovalID: approval.ID, MonthStart: month.Format("2006-01"), Generation: approval.Generation, ReceiptSchema: 1,
	})
	if err != nil {
		return opjob.Job{}, err
	}
	digest := sha256.Sum256(payload)
	return opjob.Job{
		JobType: ArchiveDeleteJobType, IdempotencyKey: "archive-delete:" + approval.ID,
		RequestHash: hex.EncodeToString(digest[:]), ProgressTotal: 1, CheckpointJSON: payload, CreatedBy: actor,
	}, nil
}

func (store *Store) ScheduleArchiveMonthDelete(ctx context.Context, approvalID string, expectedApprovalVersion uint64, actor string, now time.Time) (opjob.Job, DeletionReceipt, error) {
	if store == nil || store.db == nil || strings.TrimSpace(approvalID) == "" || expectedApprovalVersion == 0 || strings.TrimSpace(actor) == "" || now.IsZero() {
		return opjob.Job{}, DeletionReceipt{}, ErrInvalidDeletionApproval
	}
	now = now.UTC().Truncate(time.Millisecond)
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return opjob.Job{}, DeletionReceipt{}, err
	}
	defer tx.Rollback()
	approval, err := scanDeletionApproval(tx.QueryRowContext(ctx, "SELECT "+deletionApprovalColumns+" FROM flow_deletion_approvals WHERE id=? FOR UPDATE", approvalID))
	if err != nil {
		return opjob.Job{}, DeletionReceipt{}, err
	}
	if approval.RowVersion != expectedApprovalVersion {
		return opjob.Job{}, DeletionReceipt{}, ErrVersionConflict
	}
	policy, err := scanPolicy(tx.QueryRowContext(ctx, "SELECT "+policyColumns+" FROM flow_retention_policy_revisions WHERE id=? FOR UPDATE", approval.PolicyID))
	if err != nil {
		return opjob.Job{}, DeletionReceipt{}, err
	}
	if err := validateScheduledArchiveDelete(approval, policy); err != nil {
		return opjob.Job{}, DeletionReceipt{}, err
	}
	if policy.RequireBackupBeforeDelete {
		backup, err := scanBackupEvidence(tx.QueryRowContext(ctx, "SELECT "+backupEvidenceColumns+" FROM flow_backup_restore_evidence WHERE id=? FOR UPDATE", approval.BackupEvidenceID))
		if err != nil {
			return opjob.Job{}, DeletionReceipt{}, err
		}
		if !backup.covers("archive", approval.PartitionStart, approval.PartitionEnd) {
			return opjob.Job{}, DeletionReceipt{}, ErrDeleteLocked
		}
	}
	var priorReceipt string
	err = tx.QueryRowContext(ctx, "SELECT id FROM flow_deletion_receipts WHERE deletion_approval_id=? FOR UPDATE", approval.ID).Scan(&priorReceipt)
	if err == nil {
		return opjob.Job{}, DeletionReceipt{}, ErrTransition
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return opjob.Job{}, DeletionReceipt{}, err
	}
	job, err := NewArchiveDeleteOperationJob(approval, actor)
	if err != nil {
		return opjob.Job{}, DeletionReceipt{}, err
	}
	job, err = opjob.EnqueueTx(ctx, tx, job)
	if err != nil {
		return opjob.Job{}, DeletionReceipt{}, err
	}
	coverageJSON, err := json.Marshal([]OffsetCoverage{})
	if err != nil {
		return opjob.Job{}, DeletionReceipt{}, err
	}
	receiptID := newID()
	queryID := "flow-archive-delete-" + job.ID
	_, err = tx.ExecContext(ctx, `INSERT INTO flow_deletion_receipts
		(id,storage_kind,partition_granularity,partition_start,partition_end,policy_id,policy_version,generation,
		 operation_job_id,deletion_approval_id,backup_evidence_id,kafka_coverage_json,
		 source_record_count,source_physical_record_count,source_raw_bytes,source_raw_packets,source_estimated_bytes,source_estimated_packets,source_estimated_valid_records,
		 ch_query_id,status,requested_by,requested_at)
		VALUES (?,'archive','month',?,?,?,?,?,?,?,NULLIF(?,''),?,?,?,?,?,?,?,?,?,'requested',?,?)`,
		receiptID, approval.PartitionStart, approval.PartitionEnd, approval.PolicyID, approval.PolicyVersion, approval.Generation,
		job.ID, approval.ID, approval.BackupEvidenceID, coverageJSON,
		approval.Archive.RecordCount, approval.PhysicalRecords, approval.Archive.RawBytes, approval.Archive.RawPackets,
		approval.Archive.EstimatedBytes, approval.Archive.EstimatedPackets, approval.Archive.EstimatedValidRecords,
		queryID, actor, now)
	if err != nil {
		return opjob.Job{}, DeletionReceipt{}, err
	}
	if err := tx.Commit(); err != nil {
		return opjob.Job{}, DeletionReceipt{}, err
	}
	receipt, err := store.GetDeletionReceiptByJob(ctx, job.ID)
	return job, receipt, err
}

func validateScheduledArchiveDelete(approval DeletionApproval, policy Policy) error {
	_, end, err := archiveMonth(approval.PartitionStart)
	if err != nil || approval.Status != "approved" || approval.StorageKind != "archive" || approval.PartitionGranularity != "month" ||
		!approval.PartitionEnd.Equal(end) || policy.Status != PolicyPublished || !policy.ArchiveDeleteEnabled || policy.ArchiveRetentionSeconds == 0 ||
		policy.ID != approval.PolicyID || policy.Version != approval.PolicyVersion || approval.Source != approval.Archive ||
		approval.PhysicalRecords == 0 || len(approval.KafkaCoverage) != 0 {
		return ErrDeleteLocked
	}
	return nil
}

func (store *Store) LoadArchiveDeleteExecution(ctx context.Context, jobID string) (ArchiveDeleteExecution, error) {
	receipt, err := store.GetDeletionReceiptByJob(ctx, jobID)
	if err != nil {
		return ArchiveDeleteExecution{}, err
	}
	approval, err := store.GetDeletionApproval(ctx, receipt.DeletionApprovalID)
	if err != nil {
		return ArchiveDeleteExecution{}, err
	}
	policy, err := store.GetPolicy(ctx, receipt.PolicyID)
	if err != nil {
		return ArchiveDeleteExecution{}, err
	}
	return ArchiveDeleteExecution{Approval: approval, Receipt: receipt, Policy: policy}, nil
}

type ArchiveMonthDeleteRunner interface {
	ArchiveMonthEvidenceReader
	DropArchiveMonth(context.Context, time.Time, string) error
}

type archiveDeleteExecutionStore interface {
	LoadArchiveDeleteExecution(context.Context, string) (ArchiveDeleteExecution, error)
	ArchiveMonthDeleteReadiness(context.Context, time.Time, time.Time, ArchiveMonthEvidenceReader) (ArchiveMonthDeleteReadiness, error)
	CompleteArchiveMonthDelete(context.Context, string, time.Time) error
}

func NewArchiveDeleteHandler(store archiveDeleteExecutionStore, runner ArchiveMonthDeleteRunner) opjob.Handler {
	return func(ctx context.Context, job opjob.Job) (string, error) {
		if store == nil || runner == nil || job.JobType != ArchiveDeleteJobType || strings.TrimSpace(job.ID) == "" {
			return "", opjob.TerminalError(errors.New("archive Flow deletion dependencies or job identity are invalid"))
		}
		var payload ArchiveDeletePayload
		if err := opjob.DecodePayload(job.CheckpointJSON, archiveDeletePayloadSchema, &payload); err != nil {
			return "", err
		}
		month, err := time.Parse("2006-01", payload.MonthStart)
		if err != nil || payload.ApprovalID == "" || payload.Generation == 0 || payload.ReceiptSchema != 1 {
			return "", opjob.TerminalError(ErrInvalidDeletionApproval)
		}
		execution, err := store.LoadArchiveDeleteExecution(ctx, job.ID)
		if err != nil {
			return "", err
		}
		if execution.Receipt.Status == "succeeded" {
			return archiveDeleteResultRef(execution.Receipt), nil
		}
		if !archiveDeleteExecutionMatches(job, payload, month, execution) {
			return "", opjob.TerminalError(ErrDeleteLocked)
		}
		current, err := runner.ArchiveMonthStorageCounters(ctx, month)
		if err != nil {
			return "", err
		}
		physical, err := runner.ArchiveMonthPhysicalRecords(ctx, month)
		if err != nil {
			return "", err
		}
		currentArchive := lifecycleCounters(current)
		alreadyDropped := physical == 0 && currentArchive == (Counters{}) && job.AttemptCount > 1
		var dropErr error
		if !alreadyDropped {
			if physical != execution.Approval.PhysicalRecords || currentArchive != execution.Approval.Archive {
				return "", opjob.TerminalError(ErrDeleteLocked)
			}
			readiness, err := store.ArchiveMonthDeleteReadiness(ctx, month, time.Now().UTC(), runner)
			if err != nil {
				return "", err
			}
			if !archiveReadinessMatchesApproval(readiness, execution.Approval) || !readiness.DeletionReady {
				return "", opjob.TerminalError(ErrDeleteLocked)
			}
			dropErr = runner.DropArchiveMonth(ctx, month, execution.Receipt.ClickHouseQueryID)
		}
		postCounters, countersErr := runner.ArchiveMonthStorageCounters(ctx, month)
		postPhysical, physicalErr := runner.ArchiveMonthPhysicalRecords(ctx, month)
		if countersErr != nil {
			return "", countersErr
		}
		if physicalErr != nil {
			return "", physicalErr
		}
		if postPhysical != 0 || lifecycleCounters(postCounters) != (Counters{}) {
			if dropErr != nil {
				var permanent *flowch.PermanentError
				if errors.As(dropErr, &permanent) {
					return "", opjob.TerminalError(dropErr)
				}
				return "", dropErr
			}
			return "", errors.New("Flow archive partition is still visible after synchronous deletion")
		}
		if err := store.CompleteArchiveMonthDelete(ctx, job.ID, time.Now().UTC()); err != nil {
			return "", err
		}
		execution, err = store.LoadArchiveDeleteExecution(ctx, job.ID)
		if err != nil {
			return "", err
		}
		return archiveDeleteResultRef(execution.Receipt), nil
	}
}

func archiveDeleteExecutionMatches(job opjob.Job, payload ArchiveDeletePayload, month time.Time, execution ArchiveDeleteExecution) bool {
	return execution.Receipt.Status == "requested" && execution.Receipt.OperationJobID == job.ID &&
		execution.Receipt.DeletionApprovalID == payload.ApprovalID && execution.Receipt.Generation == payload.Generation &&
		execution.Receipt.PartitionStart.Equal(month) && execution.Approval.ID == payload.ApprovalID &&
		execution.Approval.Status == "approved" && execution.Policy.ID == execution.Approval.PolicyID &&
		execution.Policy.Version == execution.Approval.PolicyVersion && execution.Policy.Status == PolicyPublished &&
		execution.Policy.ArchiveDeleteEnabled && execution.Policy.ArchiveRetentionSeconds > 0
}

func (store *Store) CompleteArchiveMonthDelete(ctx context.Context, jobID string, completedAt time.Time) error {
	if store == nil || store.db == nil || strings.TrimSpace(jobID) == "" || completedAt.IsZero() {
		return ErrInvalidDeletionApproval
	}
	completedAt = completedAt.UTC().Truncate(time.Millisecond)
	result, err := store.db.ExecContext(ctx, `UPDATE flow_deletion_receipts SET status='succeeded',post_delete_record_count=0,
		completed_at=?,error_code=NULL,error_detail=NULL,row_version=row_version+1
		WHERE operation_job_id=? AND storage_kind='archive' AND status='requested'`, completedAt, jobID)
	if err != nil {
		return err
	}
	changed, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if changed == 1 {
		return nil
	}
	receipt, getErr := store.GetDeletionReceiptByJob(ctx, jobID)
	if getErr == nil && receipt.StorageKind == "archive" && receipt.Status == "succeeded" {
		return nil
	}
	return ErrTransition
}

func (store *Store) MarkArchiveDeleteTerminalFailure(ctx context.Context, jobID, code, detail string, completedAt time.Time) error {
	if store == nil || store.db == nil || strings.TrimSpace(jobID) == "" || strings.TrimSpace(code) == "" || completedAt.IsZero() {
		return ErrInvalidDeletionApproval
	}
	_, err := store.db.ExecContext(ctx, `UPDATE flow_deletion_receipts SET status='failed',completed_at=?,error_code=?,error_detail=LEFT(?,1024),row_version=row_version+1
		WHERE operation_job_id=? AND storage_kind='archive' AND status='requested'`, completedAt.UTC().Truncate(time.Millisecond), code, detail, jobID)
	return err
}

func archiveDeleteResultRef(receipt DeletionReceipt) string {
	return fmt.Sprintf("clickhouse:flow:archive-deleted:%s:g%d", receipt.PartitionStart.Format("2006-01"), receipt.Generation)
}
