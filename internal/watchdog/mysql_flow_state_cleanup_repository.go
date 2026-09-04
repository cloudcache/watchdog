package watchdog

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/cloudcache/watchdog/internal/flowcollect"
	mysqldriver "github.com/go-sql-driver/mysql"
)

const flowStateCleanupSelectColumns = `
	id, tenant_id, status, idempotency_key, request_hash, checkpoint_json,
	lease_owner, lease_token, lease_expires_at, next_attempt_at, attempt_count,
	last_error_code, last_error_detail, row_version, created_by, created_at,
	started_at, heartbeat_at, finished_at, updated_at`

type flowStateCleanupRow interface {
	Scan(...any) error
}

func (s *MySQLStore) CreateFlowStateCleanupJob(ctx context.Context, job FlowStateCleanupJob) (FlowStateCleanupJob, error) {
	if s == nil || s.db == nil || ctx == nil {
		return FlowStateCleanupJob{}, errors.New("mysql flow state-cleanup repository is required")
	}
	if err := validateNewFlowStateCleanupJob(job); err != nil {
		return FlowStateCleanupJob{}, err
	}
	checkpointJSON, err := json.Marshal(job.Snapshot)
	if err != nil {
		return FlowStateCleanupJob{}, fmt.Errorf("marshal flow state-cleanup checkpoint: %w", err)
	}
	nextAttemptAt := job.NextAttemptAt
	if nextAttemptAt.IsZero() {
		nextAttemptAt = time.Now().UTC()
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return FlowStateCleanupJob{}, err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `
		INSERT INTO operation_jobs (
			id, tenant_id, job_type, status, idempotency_key, request_hash,
			checkpoint_json, next_attempt_at, created_by
		) VALUES (?, ?, ?, 'queued', ?, ?, ?, ?, NULLIF(?, ''))
	`, job.ID, job.TenantID, FlowStateCleanupJobType, job.IdempotencyKey, job.RequestHash, checkpointJSON, nextAttemptAt, job.CreatedBy)
	if err != nil {
		var mysqlErr *mysqldriver.MySQLError
		if !errors.As(err, &mysqlErr) || mysqlErr.Number != 1062 {
			return FlowStateCleanupJob{}, err
		}
		if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, sql.ErrTxDone) {
			return FlowStateCleanupJob{}, rollbackErr
		}
		existing, findErr := s.getFlowStateCleanupJobByIdempotency(ctx, job.TenantID, job.IdempotencyKey)
		if findErr != nil {
			return FlowStateCleanupJob{}, findErr
		}
		if existing.RequestHash != job.RequestHash {
			return FlowStateCleanupJob{}, ErrFlowStateCleanupIdempotencyConflict
		}
		return existing, nil
	}
	if err := insertFlowStateCleanupAudit(ctx, tx, job.TenantID, job.CreatedBy, job.ID, "flow.state_cleanup.created", 1, map[string]any{
		"status": OperationJobQueued, "phase": job.Snapshot.Phase, "request_hash": job.RequestHash,
	}); err != nil {
		return FlowStateCleanupJob{}, err
	}
	if err := tx.Commit(); err != nil {
		return FlowStateCleanupJob{}, err
	}
	return s.GetFlowStateCleanupJob(ctx, job.TenantID, job.ID)
}

func (s *MySQLStore) GetFlowStateCleanupJob(ctx context.Context, tenantID, jobID ID) (FlowStateCleanupJob, error) {
	if s == nil || s.db == nil || ctx == nil || tenantID == "" || jobID == "" {
		return FlowStateCleanupJob{}, errors.New("tenant and flow state-cleanup job IDs are required")
	}
	return scanFlowStateCleanupJob(s.db.QueryRowContext(ctx, `SELECT `+flowStateCleanupSelectColumns+`
		FROM operation_jobs WHERE tenant_id = ? AND id = ? AND job_type = ?`, tenantID, jobID, FlowStateCleanupJobType))
}

func (s *MySQLStore) getFlowStateCleanupJobByIdempotency(ctx context.Context, tenantID ID, idempotencyKey string) (FlowStateCleanupJob, error) {
	return scanFlowStateCleanupJob(s.db.QueryRowContext(ctx, `SELECT `+flowStateCleanupSelectColumns+`
		FROM operation_jobs WHERE tenant_id = ? AND job_type = ? AND idempotency_key = ?`, tenantID, FlowStateCleanupJobType, idempotencyKey))
}

func (s *MySQLStore) GetFlowStateCleanupJobByIdempotency(ctx context.Context, tenantID ID, idempotencyKey string) (FlowStateCleanupJob, bool, error) {
	if s == nil || s.db == nil || ctx == nil || tenantID == "" || idempotencyKey == "" || len(idempotencyKey) > 128 {
		return FlowStateCleanupJob{}, false, errors.New("tenant and flow state-cleanup idempotency key are required")
	}
	job, err := s.getFlowStateCleanupJobByIdempotency(ctx, tenantID, idempotencyKey)
	if errors.Is(err, sql.ErrNoRows) {
		return FlowStateCleanupJob{}, false, nil
	}
	return job, err == nil, err
}

func (s *MySQLStore) GetFlowStateCleanupAuthority(ctx context.Context, tenantID, transferID ID) (FlowStateCleanupAuthority, error) {
	if s == nil || s.db == nil || ctx == nil || tenantID == "" || transferID == "" {
		return FlowStateCleanupAuthority{}, errors.New("tenant and collector ownership transfer IDs are required")
	}
	var authority FlowStateCleanupAuthority
	err := s.db.QueryRowContext(ctx, `
		SELECT id, tenant_id, exporter_id, old_collector_id, old_plan_revision,
			old_ownership_epoch, approval_id
		FROM collector_ownership_transfers
		WHERE tenant_id = ? AND id = ?
	`, tenantID, transferID).Scan(
		&authority.TransferID, &authority.TenantID, &authority.ExporterID,
		&authority.OldCollectorID, &authority.OldPlanRevision,
		&authority.OldOwnershipEpoch, &authority.ApprovalID,
	)
	if err != nil {
		return FlowStateCleanupAuthority{}, err
	}
	if !validFlowStateCleanupAuthority(authority, tenantID, transferID) {
		return FlowStateCleanupAuthority{}, ErrFlowStateCleanupCheckpointInvalid
	}
	return authority, nil
}

func (s *MySQLStore) ClaimFlowStateCleanupJob(ctx context.Context, leaseOwner, leaseToken string, leaseDuration time.Duration) (FlowStateCleanupJob, bool, error) {
	if s == nil || s.db == nil || ctx == nil || !validFlowStateCleanupLease(leaseOwner, leaseToken, leaseDuration) {
		return FlowStateCleanupJob{}, false, errors.New("flow state-cleanup lease owner, token, and duration are required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return FlowStateCleanupJob{}, false, err
	}
	defer tx.Rollback()
	job, err := scanFlowStateCleanupJob(tx.QueryRowContext(ctx, `SELECT `+flowStateCleanupSelectColumns+`
		FROM operation_jobs
		WHERE job_type = ?
		  AND ((status = 'queued' AND next_attempt_at <= CURRENT_TIMESTAMP(3))
		    OR (status = 'running' AND lease_expires_at <= CURRENT_TIMESTAMP(3)))
		ORDER BY next_attempt_at, id
		LIMIT 1 FOR UPDATE SKIP LOCKED`, FlowStateCleanupJobType))
	if errors.Is(err, sql.ErrNoRows) {
		return FlowStateCleanupJob{}, false, nil
	}
	if err != nil {
		return FlowStateCleanupJob{}, false, err
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE operation_jobs
		SET status = 'running', lease_owner = ?, lease_token = ?,
			lease_expires_at = TIMESTAMPADD(MICROSECOND, ?, CURRENT_TIMESTAMP(3)),
			started_at = COALESCE(started_at, CURRENT_TIMESTAMP(3)),
			heartbeat_at = CURRENT_TIMESTAMP(3), attempt_count = attempt_count + 1,
			last_error_code = NULL, last_error_detail = NULL,
			row_version = row_version + 1
		WHERE id = ? AND job_type = ? AND row_version = ?
	`, leaseOwner, leaseToken, leaseDuration.Microseconds(), job.ID, FlowStateCleanupJobType, job.RowVersion)
	if err != nil {
		return FlowStateCleanupJob{}, false, err
	}
	if err := requireOneFlowStateCleanupRow(result); err != nil {
		return FlowStateCleanupJob{}, false, err
	}
	if err := insertFlowStateCleanupAudit(ctx, tx, job.TenantID, "", job.ID, "flow.state_cleanup.claimed", job.RowVersion+1, map[string]any{
		"lease_owner": leaseOwner, "attempt": job.AttemptCount + 1, "phase": job.Snapshot.Phase,
	}); err != nil {
		return FlowStateCleanupJob{}, false, err
	}
	claimed, err := getFlowStateCleanupJobForUpdate(ctx, tx, job.ID, false)
	if err != nil {
		return FlowStateCleanupJob{}, false, err
	}
	if err := tx.Commit(); err != nil {
		return FlowStateCleanupJob{}, false, err
	}
	return claimed, true, nil
}

func (s *MySQLStore) RenewFlowStateCleanupLease(ctx context.Context, jobID ID, leaseToken string, leaseDuration time.Duration) error {
	if s == nil || s.db == nil || ctx == nil || jobID == "" || !validFlowStateCleanupLease("worker", leaseToken, leaseDuration) {
		return errors.New("flow state-cleanup job, lease token, and duration are required")
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE operation_jobs
		SET lease_expires_at = GREATEST(
				TIMESTAMPADD(MICROSECOND, ?, CURRENT_TIMESTAMP(3)),
				TIMESTAMPADD(MICROSECOND, 1000, lease_expires_at)
			),
			heartbeat_at = GREATEST(
				CURRENT_TIMESTAMP(3),
				TIMESTAMPADD(MICROSECOND, 1000, heartbeat_at)
			)
		WHERE id = ? AND job_type = ? AND status = 'running'
		  AND lease_token = ? AND lease_expires_at > CURRENT_TIMESTAMP(3)
	`, leaseDuration.Microseconds(), jobID, FlowStateCleanupJobType, leaseToken)
	if err != nil {
		return err
	}
	return requireOneFlowStateCleanupRow(result)
}

func (s *MySQLStore) SaveFlowStateCleanupCheckpoint(ctx context.Context, jobID ID, leaseToken string, expectedRowVersion uint64, snapshot flowcollect.StateCleanupSnapshot, nextAttemptAt time.Time) (FlowStateCleanupJob, error) {
	if s == nil || s.db == nil || ctx == nil || jobID == "" || leaseToken == "" || expectedRowVersion == 0 || nextAttemptAt.IsZero() {
		return FlowStateCleanupJob{}, errors.New("flow state-cleanup checkpoint lease, version, and next attempt are required")
	}
	if _, err := flowcollect.RestoreStateCleanupJob(snapshot); err != nil {
		return FlowStateCleanupJob{}, fmt.Errorf("validate flow state-cleanup checkpoint: %w", err)
	}
	checkpointJSON, err := json.Marshal(snapshot)
	if err != nil {
		return FlowStateCleanupJob{}, fmt.Errorf("marshal flow state-cleanup checkpoint: %w", err)
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return FlowStateCleanupJob{}, err
	}
	defer tx.Rollback()
	current, err := getFlowStateCleanupJobForUpdate(ctx, tx, jobID, true)
	if err != nil {
		return FlowStateCleanupJob{}, err
	}
	if current.Status != OperationJobRunning || current.LeaseToken != leaseToken || current.RowVersion != expectedRowVersion || !validFlowStateCleanupAdvance(current.Snapshot, snapshot) {
		return FlowStateCleanupJob{}, ErrFlowStateCleanupConflict
	}
	nextStatus := OperationJobRunning
	action := "flow.state_cleanup.checkpointed"
	var result sql.Result
	if snapshot.Phase == flowcollect.StateCleanupComplete {
		nextStatus = OperationJobSucceeded
		action = "flow.state_cleanup.succeeded"
		result, err = tx.ExecContext(ctx, `
			UPDATE operation_jobs
			SET status = 'succeeded', checkpoint_json = ?, next_attempt_at = ?,
				lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
				heartbeat_at = CURRENT_TIMESTAMP(3), finished_at = CURRENT_TIMESTAMP(3),
				last_error_code = NULL, last_error_detail = NULL, row_version = row_version + 1
			WHERE id = ? AND job_type = ? AND status = 'running' AND lease_token = ?
			  AND lease_expires_at > CURRENT_TIMESTAMP(3) AND row_version = ?
		`, checkpointJSON, nextAttemptAt, jobID, FlowStateCleanupJobType, leaseToken, expectedRowVersion)
	} else {
		result, err = tx.ExecContext(ctx, `
			UPDATE operation_jobs
			SET checkpoint_json = ?, next_attempt_at = ?, heartbeat_at = CURRENT_TIMESTAMP(3),
				last_error_code = NULL, last_error_detail = NULL, row_version = row_version + 1
			WHERE id = ? AND job_type = ? AND status = 'running' AND lease_token = ?
			  AND lease_expires_at > CURRENT_TIMESTAMP(3) AND row_version = ?
		`, checkpointJSON, nextAttemptAt, jobID, FlowStateCleanupJobType, leaseToken, expectedRowVersion)
	}
	if err != nil {
		return FlowStateCleanupJob{}, err
	}
	if err := requireOneFlowStateCleanupRow(result); err != nil {
		return FlowStateCleanupJob{}, err
	}
	if err := insertFlowStateCleanupAudit(ctx, tx, current.TenantID, "", jobID, action, expectedRowVersion+1, map[string]any{
		"before_status": current.Status, "after_status": nextStatus,
		"before_phase": current.Snapshot.Phase, "after_phase": snapshot.Phase,
		"lease_owner": current.LeaseOwner,
	}); err != nil {
		return FlowStateCleanupJob{}, err
	}
	updated, err := getFlowStateCleanupJobForUpdate(ctx, tx, jobID, false)
	if err != nil {
		return FlowStateCleanupJob{}, err
	}
	if err := tx.Commit(); err != nil {
		return FlowStateCleanupJob{}, err
	}
	return updated, nil
}

func (s *MySQLStore) RequeueFlowStateCleanupJob(ctx context.Context, jobID ID, leaseToken string, expectedRowVersion uint64, errorCode, errorDetail string, nextAttemptAt time.Time) (FlowStateCleanupJob, error) {
	if nextAttemptAt.IsZero() {
		return FlowStateCleanupJob{}, errors.New("flow state-cleanup retry time is required")
	}
	return s.finishFlowStateCleanupAttempt(ctx, jobID, leaseToken, expectedRowVersion, OperationJobQueued, errorCode, errorDetail, nextAttemptAt)
}

func (s *MySQLStore) FailFlowStateCleanupJob(ctx context.Context, jobID ID, leaseToken string, expectedRowVersion uint64, errorCode, errorDetail string) (FlowStateCleanupJob, error) {
	return s.finishFlowStateCleanupAttempt(ctx, jobID, leaseToken, expectedRowVersion, OperationJobFailed, errorCode, errorDetail, time.Time{})
}

func (s *MySQLStore) RetryFlowStateCleanupJob(ctx context.Context, tenantID, jobID ID, expectedRowVersion uint64, actorID ID) (FlowStateCleanupJob, error) {
	if s == nil || s.db == nil || ctx == nil || tenantID == "" || jobID == "" || expectedRowVersion == 0 || actorID == "" || len(actorID) > 26 {
		return FlowStateCleanupJob{}, errors.New("flow state-cleanup retry identity and expected version are required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return FlowStateCleanupJob{}, err
	}
	defer tx.Rollback()
	current, err := getFlowStateCleanupJobForUpdate(ctx, tx, jobID, true)
	if err != nil {
		return FlowStateCleanupJob{}, err
	}
	if current.TenantID != tenantID {
		return FlowStateCleanupJob{}, sql.ErrNoRows
	}
	if current.RowVersion != expectedRowVersion {
		return FlowStateCleanupJob{}, ErrFlowStateCleanupConflict
	}
	if current.Status != OperationJobFailed || !retryableFlowStateCleanupErrorCode(current.LastErrorCode) || current.Snapshot.Phase == flowcollect.StateCleanupComplete {
		return FlowStateCleanupJob{}, ErrFlowStateCleanupRetryNotAllowed
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE operation_jobs
		SET status = 'queued', next_attempt_at = CURRENT_TIMESTAMP(3),
			finished_at = NULL, row_version = row_version + 1,
			updated_at = CURRENT_TIMESTAMP(3)
		WHERE tenant_id = ? AND id = ? AND job_type = ? AND status = 'failed'
		  AND row_version = ?
	`, tenantID, jobID, FlowStateCleanupJobType, expectedRowVersion)
	if err != nil {
		return FlowStateCleanupJob{}, err
	}
	if err := requireOneFlowStateCleanupRow(result); err != nil {
		return FlowStateCleanupJob{}, err
	}
	if err := insertFlowStateCleanupAudit(ctx, tx, tenantID, actorID, jobID, "flow.state_cleanup.manual_retry", expectedRowVersion+1, map[string]any{
		"before_status": current.Status, "after_status": OperationJobQueued,
		"phase": current.Snapshot.Phase, "previous_error_code": current.LastErrorCode,
	}); err != nil {
		return FlowStateCleanupJob{}, err
	}
	updated, err := getFlowStateCleanupJobForUpdate(ctx, tx, jobID, false)
	if err != nil {
		return FlowStateCleanupJob{}, err
	}
	if err := tx.Commit(); err != nil {
		return FlowStateCleanupJob{}, err
	}
	return updated, nil
}

func retryableFlowStateCleanupErrorCode(code string) bool {
	switch code {
	case "FENCE_EVIDENCE_UNAVAILABLE", "FENCE_NOT_MATURE", "RESTORE_PROOF_UNAVAILABLE",
		"REPLACEMENT_NOT_OBSERVED", "STATE_SCAN_FAILED", "TOMBSTONE_PUBLISH_FAILED",
		"TOMBSTONE_NOT_VISIBLE":
		return true
	default:
		return false
	}
}

func (s *MySQLStore) finishFlowStateCleanupAttempt(ctx context.Context, jobID ID, leaseToken string, expectedRowVersion uint64, status OperationJobStatus, errorCode, errorDetail string, nextAttemptAt time.Time) (FlowStateCleanupJob, error) {
	if s == nil || s.db == nil || ctx == nil || jobID == "" || leaseToken == "" || expectedRowVersion == 0 || (status != OperationJobQueued && status != OperationJobFailed) || strings.TrimSpace(errorCode) == "" || len(errorCode) > 64 || len(errorDetail) > 1024 {
		return FlowStateCleanupJob{}, errors.New("flow state-cleanup attempt transition is invalid")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return FlowStateCleanupJob{}, err
	}
	defer tx.Rollback()
	current, err := getFlowStateCleanupJobForUpdate(ctx, tx, jobID, true)
	if err != nil {
		return FlowStateCleanupJob{}, err
	}
	if current.Status != OperationJobRunning || current.LeaseToken != leaseToken || current.RowVersion != expectedRowVersion {
		return FlowStateCleanupJob{}, ErrFlowStateCleanupConflict
	}
	action := "flow.state_cleanup.requeued"
	finishedAt := any(nil)
	if status == OperationJobFailed {
		action = "flow.state_cleanup.failed"
		finishedAt = time.Now().UTC()
	} else if nextAttemptAt.IsZero() {
		return FlowStateCleanupJob{}, errors.New("flow state-cleanup retry time is required")
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE operation_jobs
		SET status = ?, lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
			next_attempt_at = IF(? = 'queued', ?, next_attempt_at),
			last_error_code = ?, last_error_detail = NULLIF(?, ''), finished_at = ?,
			heartbeat_at = CURRENT_TIMESTAMP(3), row_version = row_version + 1
		WHERE id = ? AND job_type = ? AND status = 'running' AND lease_token = ?
		  AND lease_expires_at > CURRENT_TIMESTAMP(3) AND row_version = ?
	`, status, status, nextAttemptAt, errorCode, errorDetail, finishedAt, jobID, FlowStateCleanupJobType, leaseToken, expectedRowVersion)
	if err != nil {
		return FlowStateCleanupJob{}, err
	}
	if err := requireOneFlowStateCleanupRow(result); err != nil {
		return FlowStateCleanupJob{}, err
	}
	if err := insertFlowStateCleanupAudit(ctx, tx, current.TenantID, "", jobID, action, expectedRowVersion+1, map[string]any{
		"before_status": current.Status, "after_status": status, "phase": current.Snapshot.Phase,
		"error_code": errorCode, "lease_owner": current.LeaseOwner,
	}); err != nil {
		return FlowStateCleanupJob{}, err
	}
	updated, err := getFlowStateCleanupJobForUpdate(ctx, tx, jobID, false)
	if err != nil {
		return FlowStateCleanupJob{}, err
	}
	if err := tx.Commit(); err != nil {
		return FlowStateCleanupJob{}, err
	}
	return updated, nil
}

func getFlowStateCleanupJobForUpdate(ctx context.Context, tx *sql.Tx, jobID ID, lock bool) (FlowStateCleanupJob, error) {
	query := `SELECT ` + flowStateCleanupSelectColumns + ` FROM operation_jobs WHERE id = ? AND job_type = ?`
	if lock {
		query += ` FOR UPDATE`
	}
	return scanFlowStateCleanupJob(tx.QueryRowContext(ctx, query, jobID, FlowStateCleanupJobType))
}

func scanFlowStateCleanupJob(row flowStateCleanupRow) (FlowStateCleanupJob, error) {
	var job FlowStateCleanupJob
	var checkpointJSON []byte
	var leaseOwner, leaseToken, lastErrorCode, lastErrorDetail, createdBy sql.NullString
	var leaseExpiresAt, startedAt, heartbeatAt, finishedAt sql.NullTime
	err := row.Scan(
		&job.ID, &job.TenantID, &job.Status, &job.IdempotencyKey, &job.RequestHash, &checkpointJSON,
		&leaseOwner, &leaseToken, &leaseExpiresAt, &job.NextAttemptAt, &job.AttemptCount,
		&lastErrorCode, &lastErrorDetail, &job.RowVersion, &createdBy, &job.CreatedAt,
		&startedAt, &heartbeatAt, &finishedAt, &job.UpdatedAt,
	)
	if err != nil {
		return FlowStateCleanupJob{}, err
	}
	if err := json.Unmarshal(checkpointJSON, &job.Snapshot); err != nil {
		return FlowStateCleanupJob{}, fmt.Errorf("decode flow state-cleanup checkpoint: %w", err)
	}
	if _, err := flowcollect.RestoreStateCleanupJob(job.Snapshot); err != nil {
		return FlowStateCleanupJob{}, fmt.Errorf("validate flow state-cleanup checkpoint: %w", err)
	}
	job.LeaseOwner = leaseOwner.String
	job.LeaseToken = leaseToken.String
	job.LeaseExpiresAt = leaseExpiresAt.Time
	job.LastErrorCode = lastErrorCode.String
	job.LastErrorDetail = lastErrorDetail.String
	job.CreatedBy = ID(createdBy.String)
	job.StartedAt = startedAt.Time
	job.HeartbeatAt = heartbeatAt.Time
	job.FinishedAt = finishedAt.Time
	terminal := job.Status == OperationJobSucceeded || job.Status == OperationJobFailed || job.Status == OperationJobCanceled
	leaseComplete := job.LeaseOwner != "" && job.LeaseToken != "" && !job.LeaseExpiresAt.IsZero()
	if job.ID == "" || job.TenantID == "" || job.IdempotencyKey == "" || !validSHA256Hex(job.RequestHash) || job.Snapshot.JobID != string(job.ID) || job.Snapshot.Old.TenantID != string(job.TenantID) || job.RowVersion == 0 || job.NextAttemptAt.IsZero() || job.CreatedAt.IsZero() || job.UpdatedAt.Before(job.CreatedAt) || !validOperationJobStatus(job.Status) || (job.Status == OperationJobRunning) != leaseComplete || (job.Status != OperationJobRunning && (job.LeaseOwner != "" || job.LeaseToken != "" || !job.LeaseExpiresAt.IsZero())) || terminal != !job.FinishedAt.IsZero() || (job.Status == OperationJobSucceeded) != (job.Snapshot.Phase == flowcollect.StateCleanupComplete) {
		return FlowStateCleanupJob{}, errors.New("stored flow state-cleanup job is invalid")
	}
	return job, nil
}

func validateNewFlowStateCleanupJob(job FlowStateCleanupJob) error {
	if job.ID == "" || len(job.ID) > 26 || job.TenantID == "" || len(job.TenantID) > 26 || job.CreatedBy == "" || len(job.CreatedBy) > 26 || job.IdempotencyKey == "" || len(job.IdempotencyKey) > 128 || job.Snapshot.JobID != string(job.ID) || job.Snapshot.RequestedBy != string(job.CreatedBy) || job.Snapshot.Old.TenantID != string(job.TenantID) {
		return errors.New("flow state-cleanup job identity, tenant, requester, and idempotency key are required")
	}
	if (job.Status != "" && job.Status != OperationJobQueued) || job.Snapshot.Phase != flowcollect.StateCleanupAwaitingFence {
		return errors.New("new flow state-cleanup job must be queued")
	}
	if _, err := flowcollect.RestoreStateCleanupJob(job.Snapshot); err != nil {
		return fmt.Errorf("validate flow state-cleanup checkpoint: %w", err)
	}
	expectedHash, err := FlowStateCleanupRequestHash(job.Snapshot)
	if err != nil {
		return err
	}
	if !validSHA256Hex(job.RequestHash) || job.RequestHash != expectedHash {
		return errors.New("flow state-cleanup request hash does not match the immutable request")
	}
	return nil
}

// FlowStateCleanupRequestHash binds an idempotency key to the immutable
// cleanup request. Generated job identity and timestamps are intentionally
// excluded so an HTTP retry may safely construct a fresh in-memory job.
func FlowStateCleanupRequestHash(snapshot flowcollect.StateCleanupSnapshot) (string, error) {
	if _, err := flowcollect.RestoreStateCleanupJob(snapshot); err != nil {
		return "", fmt.Errorf("validate flow state-cleanup request: %w", err)
	}
	canonical := struct {
		SchemaVersion uint32                             `json:"schema_version"`
		ApprovalID    string                             `json:"approval_id"`
		RequestedBy   string                             `json:"requested_by"`
		Old           flowcollect.StateCleanupCheckpoint `json:"old"`
	}{
		SchemaVersion: snapshot.SchemaVersion,
		ApprovalID:    snapshot.ApprovalID,
		RequestedBy:   snapshot.RequestedBy,
		Old:           snapshot.Old,
	}
	payload, err := json.Marshal(canonical)
	if err != nil {
		return "", fmt.Errorf("marshal flow state-cleanup request identity: %w", err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func validSHA256Hex(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && value == strings.ToLower(value)
}

func validFlowStateCleanupLease(owner, token string, duration time.Duration) bool {
	return owner != "" && len(owner) <= 128 && token != "" && len(token) <= 64 && duration > 0 && duration <= 24*time.Hour
}

func validFlowStateCleanupAdvance(current, next flowcollect.StateCleanupSnapshot) bool {
	if current.JobID != next.JobID || current.ApprovalID != next.ApprovalID || current.RequestedBy != next.RequestedBy || !current.CreatedAt.Equal(next.CreatedAt) || !equalFlowStateCleanupCheckpoint(current.Old, next.Old) || next.UpdatedAt.Before(current.UpdatedAt) {
		return false
	}
	return flowStateCleanupPhaseRank(next.Phase) == flowStateCleanupPhaseRank(current.Phase)+1
}

func equalFlowStateCleanupCheckpoint(left, right flowcollect.StateCleanupCheckpoint) bool {
	return left.Kind == right.Kind && bytes.Equal(left.KafkaKey, right.KafkaKey) && bytes.Equal(left.IdentityKey, right.IdentityKey) && bytes.Equal(left.PayloadSHA256, right.PayloadSHA256) && left.TenantID == right.TenantID && left.ExporterID == right.ExporterID && left.CollectorID == right.CollectorID && left.RegistryVersion == right.RegistryVersion && left.OwnershipEpoch == right.OwnershipEpoch && left.StateGeneration == right.StateGeneration && left.CheckpointAt.Equal(right.CheckpointAt)
}

func flowStateCleanupPhaseRank(phase flowcollect.StateCleanupPhase) int {
	switch phase {
	case flowcollect.StateCleanupAwaitingFence:
		return 1
	case flowcollect.StateCleanupAwaitingReplacement:
		return 2
	case flowcollect.StateCleanupReadyToTombstone:
		return 3
	case flowcollect.StateCleanupAwaitingVerification:
		return 4
	case flowcollect.StateCleanupComplete:
		return 5
	default:
		return -1
	}
}

func validOperationJobStatus(status OperationJobStatus) bool {
	switch status {
	case OperationJobQueued, OperationJobRunning, OperationJobSucceeded, OperationJobFailed, OperationJobCanceled:
		return true
	default:
		return false
	}
}

func requireOneFlowStateCleanupRow(result sql.Result) error {
	if result == nil {
		return ErrFlowStateCleanupConflict
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return ErrFlowStateCleanupConflict
	}
	return nil
}

func insertFlowStateCleanupAudit(ctx context.Context, tx *sql.Tx, tenantID, actorID, jobID ID, action string, rowVersion uint64, detail map[string]any) error {
	if detail == nil {
		detail = make(map[string]any)
	}
	detail["job_type"] = FlowStateCleanupJobType
	detail["row_version"] = rowVersion
	detailJSON, err := json.Marshal(detail)
	if err != nil {
		return fmt.Errorf("marshal flow state-cleanup audit: %w", err)
	}
	auditID := collectorStableID("audit", string(tenantID), string(jobID), action, fmt.Sprint(rowVersion))
	_, err = tx.ExecContext(ctx, `
		INSERT INTO audit_logs (id, tenant_id, actor_id, action, resource_type, resource_id, detail_json)
		VALUES (?, ?, NULLIF(?, ''), ?, 'operation_job', ?, ?)
	`, auditID, tenantID, actorID, action, jobID, detailJSON)
	return err
}

var _ FlowStateCleanupRepository = (*MySQLStore)(nil)
var _ FlowStateCleanupControlRepository = (*MySQLStore)(nil)
