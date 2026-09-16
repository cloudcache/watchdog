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
	"slices"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/flowch"
	"github.com/cloudcache/watchdog/internal/opjob"
)

const (
	RawDeleteJobType       = "flow_storage_raw_delete"
	rawDeletePayloadSchema = 1
)

type RawDeletePayload struct {
	ApprovalID    string `json:"approval_id"`
	SourceDate    string `json:"source_date"`
	Generation    uint64 `json:"generation"`
	ReceiptSchema uint16 `json:"receipt_schema"`
}

type DeletionReceipt struct {
	ID                    string           `json:"id"`
	StorageKind           string           `json:"storage_kind"`
	PartitionGranularity  string           `json:"partition_granularity"`
	PartitionStart        time.Time        `json:"partition_start"`
	PartitionEnd          time.Time        `json:"partition_end"`
	PolicyID              string           `json:"policy_id"`
	PolicyVersion         uint64           `json:"policy_version"`
	Generation            uint64           `json:"generation"`
	OperationJobID        string           `json:"operation_job_id"`
	DeletionApprovalID    string           `json:"deletion_approval_id"`
	BackupEvidenceID      string           `json:"backup_evidence_id,omitempty"`
	KafkaCoverage         []OffsetCoverage `json:"kafka_coverage"`
	Source                Counters         `json:"source"`
	PhysicalRecords       uint64           `json:"physical_record_count"`
	ClickHouseQueryID     string           `json:"ch_query_id"`
	PostDeleteRecordCount uint64           `json:"post_delete_record_count,omitempty"`
	Status                string           `json:"status"`
	RequestedBy           string           `json:"requested_by,omitempty"`
	RequestedAt           time.Time        `json:"requested_at"`
	CompletedAt           time.Time        `json:"completed_at,omitzero"`
	ErrorCode             string           `json:"error_code,omitempty"`
	ErrorDetail           string           `json:"error_detail,omitempty"`
	RowVersion            uint64           `json:"row_version"`
	CreatedAt             time.Time        `json:"created_at"`
}

type RawDeleteExecution struct {
	Approval DeletionApproval
	Receipt  DeletionReceipt
	State    PartitionState
	Policy   Policy
}

const deletionReceiptColumns = `id,storage_kind,partition_granularity,partition_start,partition_end,
	policy_id,policy_version,generation,operation_job_id,COALESCE(deletion_approval_id,''),COALESCE(backup_evidence_id,''),kafka_coverage_json,
	source_record_count,source_physical_record_count,source_raw_bytes,source_raw_packets,source_estimated_bytes,source_estimated_packets,source_estimated_valid_records,
	ch_query_id,COALESCE(post_delete_record_count,0),status,COALESCE(requested_by,''),requested_at,completed_at,
	COALESCE(error_code,''),COALESCE(error_detail,''),row_version,created_at`

func scanDeletionReceipt(row rowScanner) (DeletionReceipt, error) {
	var receipt DeletionReceipt
	var coverage []byte
	var completedAt sql.NullTime
	err := row.Scan(&receipt.ID, &receipt.StorageKind, &receipt.PartitionGranularity, &receipt.PartitionStart, &receipt.PartitionEnd,
		&receipt.PolicyID, &receipt.PolicyVersion, &receipt.Generation, &receipt.OperationJobID, &receipt.DeletionApprovalID, &receipt.BackupEvidenceID, &coverage,
		&receipt.Source.RecordCount, &receipt.PhysicalRecords, &receipt.Source.RawBytes, &receipt.Source.RawPackets, &receipt.Source.EstimatedBytes, &receipt.Source.EstimatedPackets, &receipt.Source.EstimatedValidRecords,
		&receipt.ClickHouseQueryID, &receipt.PostDeleteRecordCount, &receipt.Status, &receipt.RequestedBy, &receipt.RequestedAt, &completedAt,
		&receipt.ErrorCode, &receipt.ErrorDetail, &receipt.RowVersion, &receipt.CreatedAt)
	if err == nil {
		err = json.Unmarshal(coverage, &receipt.KafkaCoverage)
	}
	if completedAt.Valid {
		receipt.CompletedAt = completedAt.Time
	}
	return receipt, err
}

func NewRawDeleteOperationJob(approval DeletionApproval, actor string) (opjob.Job, error) {
	day := UTCDate(approval.PartitionStart)
	if approval.ID == "" || approval.Status != "approved" || approval.StorageKind != "raw" || approval.PartitionGranularity != "day" ||
		day.IsZero() || !approval.PartitionStart.Equal(day) || !approval.PartitionEnd.Equal(day.Add(24*time.Hour)) ||
		approval.Generation == 0 || strings.TrimSpace(actor) == "" {
		return opjob.Job{}, ErrInvalidDeletionApproval
	}
	payload, err := opjob.EncodePayload(rawDeletePayloadSchema, RawDeletePayload{
		ApprovalID: approval.ID, SourceDate: day.Format(time.DateOnly), Generation: approval.Generation, ReceiptSchema: 1,
	})
	if err != nil {
		return opjob.Job{}, err
	}
	digest := sha256.Sum256(payload)
	return opjob.Job{
		JobType: RawDeleteJobType, IdempotencyKey: "raw-delete:" + approval.ID,
		RequestHash: hex.EncodeToString(digest[:]), ProgressTotal: 1, CheckpointJSON: payload, CreatedBy: actor,
	}, nil
}

// ScheduleRawDayDelete atomically creates the canonical operation job, binds
// it to the approved partition, and writes the pre-execution receipt. The
// worker cannot observe a job without its frozen evidence.
func (store *Store) ScheduleRawDayDelete(ctx context.Context, approvalID string, expectedApprovalVersion uint64, actor string, now time.Time) (opjob.Job, DeletionReceipt, PartitionState, error) {
	if store == nil || store.db == nil || strings.TrimSpace(approvalID) == "" || expectedApprovalVersion == 0 || strings.TrimSpace(actor) == "" || now.IsZero() {
		return opjob.Job{}, DeletionReceipt{}, PartitionState{}, ErrInvalidDeletionApproval
	}
	now = now.UTC().Truncate(time.Millisecond)
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return opjob.Job{}, DeletionReceipt{}, PartitionState{}, err
	}
	defer tx.Rollback()
	approval, err := scanDeletionApproval(tx.QueryRowContext(ctx, "SELECT "+deletionApprovalColumns+" FROM flow_deletion_approvals WHERE id=? FOR UPDATE", approvalID))
	if err != nil {
		return opjob.Job{}, DeletionReceipt{}, PartitionState{}, err
	}
	if approval.RowVersion != expectedApprovalVersion {
		return opjob.Job{}, DeletionReceipt{}, PartitionState{}, ErrVersionConflict
	}
	state, err := scanPartition(tx.QueryRowContext(ctx, "SELECT "+partitionColumns+" FROM flow_retention_partition_states WHERE source_date=? FOR UPDATE", approval.PartitionStart))
	if err != nil {
		return opjob.Job{}, DeletionReceipt{}, PartitionState{}, err
	}
	policy, err := scanPolicy(tx.QueryRowContext(ctx, "SELECT "+policyColumns+" FROM flow_retention_policy_revisions WHERE id=? FOR UPDATE", approval.PolicyID))
	if err != nil {
		return opjob.Job{}, DeletionReceipt{}, PartitionState{}, err
	}
	if err := validateScheduledRawDelete(approval, state, policy); err != nil {
		return opjob.Job{}, DeletionReceipt{}, PartitionState{}, err
	}
	if err := rawDeleteBarrierReadyTx(ctx, tx, approval.PartitionStart); err != nil {
		return opjob.Job{}, DeletionReceipt{}, PartitionState{}, err
	}
	if policy.RequireBackupBeforeDelete {
		backup, err := scanBackupEvidence(tx.QueryRowContext(ctx, "SELECT "+backupEvidenceColumns+" FROM flow_backup_restore_evidence WHERE id=? FOR UPDATE", approval.BackupEvidenceID))
		if err != nil {
			return opjob.Job{}, DeletionReceipt{}, PartitionState{}, err
		}
		if !backup.covers("raw", approval.PartitionStart, approval.PartitionEnd) {
			return opjob.Job{}, DeletionReceipt{}, PartitionState{}, ErrDeleteLocked
		}
	}
	job, err := NewRawDeleteOperationJob(approval, actor)
	if err != nil {
		return opjob.Job{}, DeletionReceipt{}, PartitionState{}, err
	}
	job, err = opjob.EnqueueTx(ctx, tx, job)
	if err != nil {
		return opjob.Job{}, DeletionReceipt{}, PartitionState{}, err
	}
	coverageJSON, err := json.Marshal(approval.KafkaCoverage)
	if err != nil {
		return opjob.Job{}, DeletionReceipt{}, PartitionState{}, err
	}
	receiptID := newID()
	queryID := "flow-raw-delete-" + job.ID
	_, err = tx.ExecContext(ctx, `INSERT INTO flow_deletion_receipts
		(id,storage_kind,partition_granularity,partition_start,partition_end,policy_id,policy_version,generation,
		 operation_job_id,deletion_approval_id,backup_evidence_id,kafka_coverage_json,
		 source_record_count,source_physical_record_count,source_raw_bytes,source_raw_packets,source_estimated_bytes,source_estimated_packets,source_estimated_valid_records,
		 ch_query_id,status,requested_by,requested_at)
		VALUES (?,'raw','day',?,?,?,?,?,?,?,NULLIF(?,''),?,?,?,?,?,?,?,?,?,'requested',?,?)`,
		receiptID, approval.PartitionStart, approval.PartitionEnd, approval.PolicyID, approval.PolicyVersion, approval.Generation,
		job.ID, approval.ID, approval.BackupEvidenceID, coverageJSON,
		approval.Source.RecordCount, approval.PhysicalRecords, approval.Source.RawBytes, approval.Source.RawPackets,
		approval.Source.EstimatedBytes, approval.Source.EstimatedPackets, approval.Source.EstimatedValidRecords,
		queryID, actor, now)
	if err != nil {
		return opjob.Job{}, DeletionReceipt{}, PartitionState{}, err
	}
	result, err := tx.ExecContext(ctx, `UPDATE flow_retention_partition_states SET delete_job_id=?,row_version=row_version+1
		WHERE source_date=? AND state='delete_eligible' AND delete_approval_id=? AND delete_job_id IS NULL`, job.ID, approval.PartitionStart, approval.ID)
	if err != nil {
		return opjob.Job{}, DeletionReceipt{}, PartitionState{}, err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return opjob.Job{}, DeletionReceipt{}, PartitionState{}, err
		}
		return opjob.Job{}, DeletionReceipt{}, PartitionState{}, ErrTransition
	}
	if err := tx.Commit(); err != nil {
		return opjob.Job{}, DeletionReceipt{}, PartitionState{}, err
	}
	receipt, err := store.GetDeletionReceiptByJob(ctx, job.ID)
	if err != nil {
		return opjob.Job{}, DeletionReceipt{}, PartitionState{}, err
	}
	state, err = store.GetPartition(ctx, approval.PartitionStart)
	return job, receipt, state, err
}

func validateScheduledRawDelete(approval DeletionApproval, state PartitionState, policy Policy) error {
	if approval.Status != "approved" || approval.StorageKind != "raw" || approval.PartitionGranularity != "day" ||
		policy.Status != PolicyPublished || !policy.RawDeleteEnabled || policy.ID != approval.PolicyID || policy.Version != approval.PolicyVersion ||
		state.State != PartitionDeleteEligible || state.DeleteApprovalID != approval.ID || state.DeleteJobID != "" ||
		!state.SourceDate.Equal(approval.PartitionStart) || state.PolicyID != approval.PolicyID || state.PolicyVersion != approval.PolicyVersion ||
		state.Generation != approval.Generation || state.Source != approval.Source || state.Archive != approval.Archive {
		return ErrDeleteLocked
	}
	return nil
}

func (store *Store) GetDeletionReceiptByJob(ctx context.Context, jobID string) (DeletionReceipt, error) {
	if store == nil || store.db == nil || strings.TrimSpace(jobID) == "" {
		return DeletionReceipt{}, ErrInvalidDeletionApproval
	}
	return scanDeletionReceipt(store.db.QueryRowContext(ctx, "SELECT "+deletionReceiptColumns+" FROM flow_deletion_receipts WHERE operation_job_id=?", jobID))
}

func (store *Store) LoadRawDeleteExecution(ctx context.Context, jobID string) (RawDeleteExecution, error) {
	receipt, err := store.GetDeletionReceiptByJob(ctx, jobID)
	if err != nil {
		return RawDeleteExecution{}, err
	}
	approval, err := store.GetDeletionApproval(ctx, receipt.DeletionApprovalID)
	if err != nil {
		return RawDeleteExecution{}, err
	}
	state, err := store.GetPartition(ctx, receipt.PartitionStart)
	if err != nil {
		return RawDeleteExecution{}, err
	}
	policy, err := store.GetPolicy(ctx, receipt.PolicyID)
	if err != nil {
		return RawDeleteExecution{}, err
	}
	return RawDeleteExecution{Approval: approval, Receipt: receipt, State: state, Policy: policy}, nil
}

type RawDayDeleteRunner interface {
	RawDayEvidenceReader
	DropRawDay(context.Context, time.Time, string) error
}

type rawDeleteExecutionStore interface {
	LoadRawDeleteExecution(context.Context, string) (RawDeleteExecution, error)
	RawDayDeleteReadiness(context.Context, time.Time, time.Time, RawDayEvidenceReader) (RawDeleteReadiness, error)
	CompleteRawDayDelete(context.Context, string, time.Time) error
	RawDeleteBarrierReadyForDay(context.Context, time.Time) (DeleteBarrierStatus, error)
	EnsureNoActiveReclassificationForDay(context.Context, time.Time) error
}

func NewRawDeleteHandler(store rawDeleteExecutionStore, runner RawDayDeleteRunner) opjob.Handler {
	return func(ctx context.Context, job opjob.Job) (string, error) {
		if store == nil || runner == nil || job.JobType != RawDeleteJobType || strings.TrimSpace(job.ID) == "" {
			return "", opjob.TerminalError(errors.New("raw Flow deletion dependencies or job identity are invalid"))
		}
		var payload RawDeletePayload
		if err := opjob.DecodePayload(job.CheckpointJSON, rawDeletePayloadSchema, &payload); err != nil {
			return "", err
		}
		day, err := time.Parse(time.DateOnly, payload.SourceDate)
		if err != nil || payload.ApprovalID == "" || payload.Generation == 0 || payload.ReceiptSchema != 1 {
			return "", opjob.TerminalError(ErrInvalidDeletionApproval)
		}
		execution, err := store.LoadRawDeleteExecution(ctx, job.ID)
		if err != nil {
			return "", err
		}
		if execution.Receipt.Status == "succeeded" && execution.State.State == PartitionRawDeleted {
			return rawDeleteResultRef(execution.Receipt), nil
		}
		if !rawDeleteExecutionMatches(job, payload, day, execution) {
			return "", opjob.TerminalError(ErrDeleteLocked)
		}
		if _, err := store.RawDeleteBarrierReadyForDay(ctx, day); err != nil {
			return "", err
		}
		if err := store.EnsureNoActiveReclassificationForDay(ctx, day); err != nil {
			return "", err
		}
		raw, archive, err := runner.DayStorageCounters(ctx, day)
		if err != nil {
			return "", err
		}
		physical, err := runner.RawDayPhysicalRecords(ctx, day)
		if err != nil {
			return "", err
		}
		currentRaw, currentArchive := lifecycleCounters(raw), lifecycleCounters(archive)
		if currentArchive != execution.Approval.Archive {
			return "", opjob.TerminalError(ErrDeleteLocked)
		}
		alreadyDropped := physical == 0 && currentRaw == (Counters{}) && job.AttemptCount > 1
		var dropErr error
		if !alreadyDropped {
			if physical != execution.Approval.PhysicalRecords || currentRaw != execution.Approval.Source {
				return "", opjob.TerminalError(ErrDeleteLocked)
			}
			readiness, err := store.RawDayDeleteReadiness(ctx, day, time.Now().UTC(), runner)
			if err != nil {
				return "", err
			}
			if !readinessMatchesApproval(readiness, execution.Approval) || !readiness.DeletionReady {
				return "", opjob.TerminalError(ErrDeleteLocked)
			}
			dropErr = runner.DropRawDay(ctx, day, execution.Receipt.ClickHouseQueryID)
		}
		postRaw, postArchive, countersErr := runner.DayStorageCounters(ctx, day)
		postPhysical, physicalErr := runner.RawDayPhysicalRecords(ctx, day)
		if countersErr != nil {
			return "", countersErr
		}
		if physicalErr != nil {
			return "", physicalErr
		}
		if postPhysical != 0 || lifecycleCounters(postRaw) != (Counters{}) || lifecycleCounters(postArchive) != execution.Approval.Archive {
			if dropErr != nil {
				var permanent *flowch.PermanentError
				if errors.As(dropErr, &permanent) {
					return "", opjob.TerminalError(dropErr)
				}
				return "", dropErr
			}
			return "", errors.New("raw Flow partition is still visible after synchronous deletion")
		}
		if err := store.CompleteRawDayDelete(ctx, job.ID, time.Now().UTC()); err != nil {
			return "", err
		}
		execution, err = store.LoadRawDeleteExecution(ctx, job.ID)
		if err != nil {
			return "", err
		}
		return rawDeleteResultRef(execution.Receipt), nil
	}
}

// EnsureNoActiveReclassificationForDay prevents physical raw deletion while
// an overlapping historical reclassification still depends on those facts.
func (store *Store) EnsureNoActiveReclassificationForDay(ctx context.Context, day time.Time) error {
	if store == nil || store.db == nil {
		return ErrDeleteLocked
	}
	day = UTCDate(day)
	var count uint64
	if err := store.db.QueryRowContext(ctx, `SELECT COUNT(*)
FROM flow_reclassifications r
JOIN operation_jobs j ON j.id=r.operation_job_id
WHERE r.window_start < ? AND r.window_end > ? AND r.activated_at IS NULL
  AND j.status IN ('queued','running','cancel_requested')`, day.Add(24*time.Hour), day).Scan(&count); err != nil {
		return err
	}
	if count != 0 {
		return ErrDeleteLocked
	}
	return nil
}

func rawDeleteExecutionMatches(job opjob.Job, payload RawDeletePayload, day time.Time, execution RawDeleteExecution) bool {
	return execution.Receipt.Status == "requested" && execution.Receipt.OperationJobID == job.ID &&
		execution.Receipt.DeletionApprovalID == payload.ApprovalID && execution.Receipt.Generation == payload.Generation &&
		execution.Receipt.PartitionStart.Equal(day) && execution.Approval.ID == payload.ApprovalID &&
		execution.Approval.Status == "approved" && execution.State.State == PartitionDeleteEligible &&
		execution.State.DeleteJobID == job.ID && execution.State.DeleteApprovalID == execution.Approval.ID &&
		execution.Policy.ID == execution.Approval.PolicyID && execution.Policy.Version == execution.Approval.PolicyVersion &&
		execution.Policy.Status == PolicyPublished && execution.Policy.RawDeleteEnabled
}

func readinessMatchesApproval(readiness RawDeleteReadiness, approval DeletionApproval) bool {
	return readiness.EvidenceReady && readiness.PolicyID == approval.PolicyID && readiness.PolicyVersion == approval.PolicyVersion &&
		readiness.Generation == approval.Generation && readiness.DeleteApprovalID == approval.ID &&
		readiness.Source == approval.Source && readiness.Archive == approval.Archive && readiness.PhysicalRecords == approval.PhysicalRecords &&
		readiness.BackupEvidenceID == approval.BackupEvidenceID && slices.Equal(readiness.Coverage, approval.KafkaCoverage)
}

func (store *Store) CompleteRawDayDelete(ctx context.Context, jobID string, completedAt time.Time) error {
	if store == nil || store.db == nil || strings.TrimSpace(jobID) == "" || completedAt.IsZero() {
		return ErrInvalidDeletionApproval
	}
	completedAt = completedAt.UTC().Truncate(time.Millisecond)
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	receipt, err := scanDeletionReceipt(tx.QueryRowContext(ctx, "SELECT "+deletionReceiptColumns+" FROM flow_deletion_receipts WHERE operation_job_id=? FOR UPDATE", jobID))
	if err != nil {
		return err
	}
	state, err := scanPartition(tx.QueryRowContext(ctx, "SELECT "+partitionColumns+" FROM flow_retention_partition_states WHERE source_date=? FOR UPDATE", receipt.PartitionStart))
	if err != nil {
		return err
	}
	if receipt.Status == "succeeded" && state.State == PartitionRawDeleted && state.DeleteJobID == jobID {
		return tx.Commit()
	}
	if receipt.Status != "requested" || state.State != PartitionDeleteEligible || state.DeleteJobID != jobID || state.DeleteApprovalID != receipt.DeletionApprovalID {
		return ErrTransition
	}
	result, err := tx.ExecContext(ctx, `UPDATE flow_deletion_receipts SET status='succeeded',post_delete_record_count=0,
		completed_at=?,error_code=NULL,error_detail=NULL,row_version=row_version+1 WHERE operation_job_id=? AND status='requested'`, completedAt, jobID)
	if err != nil {
		return err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return err
		}
		return ErrTransition
	}
	result, err = tx.ExecContext(ctx, `UPDATE flow_retention_partition_states SET state='raw_deleted',raw_deleted_at=?,
		last_error_code=NULL,last_error_detail=NULL,row_version=row_version+1
		WHERE source_date=? AND state='delete_eligible' AND delete_job_id=? AND delete_approval_id=?`, completedAt, receipt.PartitionStart, jobID, receipt.DeletionApprovalID)
	if err != nil {
		return err
	}
	if changed, err := result.RowsAffected(); err != nil || changed != 1 {
		if err != nil {
			return err
		}
		return ErrTransition
	}
	return tx.Commit()
}

func (store *Store) MarkRawDeleteTerminalFailure(ctx context.Context, jobID, code, detail string, completedAt time.Time) error {
	if store == nil || store.db == nil || jobID == "" || code == "" || completedAt.IsZero() {
		return ErrInvalidDeletionApproval
	}
	completedAt = completedAt.UTC().Truncate(time.Millisecond)
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	receipt, err := scanDeletionReceipt(tx.QueryRowContext(ctx, "SELECT "+deletionReceiptColumns+" FROM flow_deletion_receipts WHERE operation_job_id=? FOR UPDATE", jobID))
	if err != nil {
		return err
	}
	if receipt.Status != "requested" {
		return tx.Commit()
	}
	if _, err := tx.ExecContext(ctx, `UPDATE flow_deletion_receipts SET status='failed',completed_at=?,error_code=?,error_detail=LEFT(?,1024),row_version=row_version+1
		WHERE operation_job_id=? AND status='requested'`, completedAt, code, detail, jobID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE flow_retention_partition_states SET state='failed',last_error_code=?,last_error_detail=LEFT(?,1024),row_version=row_version+1
		WHERE source_date=? AND delete_job_id=? AND state='delete_eligible'`, code, detail, receipt.PartitionStart, jobID); err != nil {
		return err
	}
	return tx.Commit()
}

func rawDeleteResultRef(receipt DeletionReceipt) string {
	return fmt.Sprintf("clickhouse:flow:raw-deleted:%s:g%d", receipt.PartitionStart.Format(time.DateOnly), receipt.Generation)
}
