package watchdog

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"
)

// PLAT-00A async jobs on the operation_jobs table (migration 017): enqueue is
// idempotent per (tenant, job_type, idempotency_key); execution is claimed
// with a lease (SKIP LOCKED) so exactly one worker runs a job at a time and a
// crashed worker's job is taken over after its lease expires; every attempt
// heartbeats, sees cancellation, and finishes as succeeded / retried /
// terminally failed / canceled. Schema CHECKs enforce that a lease exists
// exactly while a job runs and terminal states carry finished_at.

var (
	ErrOperationJobLeaseLost    = errors.New("operation job lease is no longer held")
	ErrOperationJobHashMismatch = errors.New("idempotency key was already used with a different request")
)

const (
	OperationJobStatusQueued          = "queued"
	OperationJobStatusRunning         = "running"
	OperationJobStatusCancelRequested = "cancel_requested"
	OperationJobStatusSucceeded       = "succeeded"
	OperationJobStatusFailed          = "failed"
	OperationJobStatusCanceled        = "canceled"
)

const (
	// OperationJobScopeTenant is the default: the job belongs to one tenant and
	// its tenant_id foreign key is enforced.
	OperationJobScopeTenant = "tenant"
	// OperationJobScopeSystem is a platform-level job with no owning tenant
	// (Kafka partition maintenance, global retention, cross-tenant receipts).
	// Its tenant_id is NULL — no fabricated "system tenant" — and its
	// idempotency key lives in a dedicated naming domain so system keys dedupe
	// among themselves without colliding with any tenant's keys. It leases,
	// retries and reports through the very same state machine as a tenant job.
	OperationJobScopeSystem = "system"
)

type OperationJob struct {
	ID                ID              `json:"id"`
	TenantID          ID              `json:"tenant_id,omitempty"`
	ScopeType         string          `json:"scope_type,omitempty"`
	ScheduleID        ID              `json:"schedule_id,omitempty"`
	JobType           string          `json:"job_type"`
	Status            string          `json:"status"`
	IdempotencyKey    string          `json:"idempotency_key"`
	RequestHash       string          `json:"request_hash"`
	ProgressDone      uint64          `json:"progress_done"`
	CheckpointJSON    json.RawMessage `json:"checkpoint,omitempty"`
	ResultRef         string          `json:"result_ref,omitempty"`
	LeaseOwner        string          `json:"lease_owner,omitempty"`
	LeaseToken        string          `json:"-"`
	LeaseExpiresAt    time.Time       `json:"lease_expires_at,omitzero"`
	NextAttemptAt     time.Time       `json:"next_attempt_at"`
	AttemptCount      uint32          `json:"attempt_count"`
	LastErrorCode     string          `json:"last_error_code,omitempty"`
	LastErrorDetail   string          `json:"last_error_detail,omitempty"`
	RowVersion        uint64          `json:"row_version"`
	CreatedBy         ID              `json:"created_by,omitempty"`
	CreatedAt         time.Time       `json:"created_at"`
	StartedAt         time.Time       `json:"started_at,omitzero"`
	CancelRequestedAt time.Time       `json:"cancel_requested_at,omitzero"`
	FinishedAt        time.Time       `json:"finished_at,omitzero"`
}

func (job OperationJob) Terminal() bool {
	switch job.Status {
	case OperationJobStatusSucceeded, OperationJobStatusFailed, OperationJobStatusCanceled:
		return true
	}
	return false
}

type OperationJobRepository interface {
	// EnqueueOperationJob creates the job or, when the (tenant, type,
	// idempotency key) triple already exists with the same request hash,
	// returns the existing job; a differing hash is a conflict.
	EnqueueOperationJob(ctx context.Context, job OperationJob) (OperationJob, error)
	GetOperationJob(ctx context.Context, tenantID, jobID ID) (OperationJob, error)
	ListOperationJobs(ctx context.Context, tenantID ID, filter OperationJobFilter) ([]OperationJob, string, error)
	// LeaseNextOperationJob claims the next due job of the type: a queued job
	// whose next_attempt_at has passed, or a running job whose lease expired
	// (takeover). sql.ErrNoRows when nothing is due.
	LeaseNextOperationJob(ctx context.Context, jobType, owner string, leaseFor time.Duration) (OperationJob, error)
	// HeartbeatOperationJob extends the lease and reports whether cancellation
	// was requested. ErrOperationJobLeaseLost when the token no longer holds.
	HeartbeatOperationJob(ctx context.Context, jobID ID, leaseToken string, leaseFor time.Duration, progressDone uint64, checkpoint json.RawMessage) (cancelRequested bool, err error)
	CompleteOperationJobSucceeded(ctx context.Context, jobID ID, leaseToken, resultRef string) error
	CompleteOperationJobCanceled(ctx context.Context, jobID ID, leaseToken string) error
	// CompleteOperationJobFailed either requeues the job for retryAt (retry
	// true) or finishes it terminally.
	CompleteOperationJobFailed(ctx context.Context, jobID ID, leaseToken, errorCode, errorDetail string, retry bool, retryAt time.Time) error
	RequestOperationJobCancel(ctx context.Context, tenantID, jobID ID) error
}

func newOperationJobToken() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

const operationJobColumns = `
	id, tenant_id, schedule_id, job_type, status, idempotency_key, request_hash,
	progress_done, checkpoint_json, COALESCE(result_ref, ''),
	COALESCE(lease_owner, ''), COALESCE(lease_token, ''), lease_expires_at,
	next_attempt_at, attempt_count, COALESCE(last_error_code, ''), COALESCE(last_error_detail, ''),
	row_version, COALESCE(created_by, ''), created_at, started_at, cancel_requested_at, finished_at, scope_type`

func scanOperationJob(row rowScanner) (OperationJob, error) {
	var job OperationJob
	var tenantID, scheduleID sql.NullString
	var leaseExpires, startedAt, cancelAt, finishedAt sql.NullTime
	var checkpoint []byte
	err := row.Scan(&job.ID, &tenantID, &scheduleID, &job.JobType, &job.Status, &job.IdempotencyKey, &job.RequestHash,
		&job.ProgressDone, &checkpoint, &job.ResultRef,
		&job.LeaseOwner, &job.LeaseToken, &leaseExpires,
		&job.NextAttemptAt, &job.AttemptCount, &job.LastErrorCode, &job.LastErrorDetail,
		&job.RowVersion, &job.CreatedBy, &job.CreatedAt, &startedAt, &cancelAt, &finishedAt, &job.ScopeType)
	if err != nil {
		return OperationJob{}, err
	}
	// tenant_id is NULL for system-scope jobs.
	job.TenantID = ID(tenantID.String)
	job.ScheduleID = ID(scheduleID.String)
	job.CheckpointJSON = checkpoint
	if leaseExpires.Valid {
		job.LeaseExpiresAt = leaseExpires.Time
	}
	if startedAt.Valid {
		job.StartedAt = startedAt.Time
	}
	if cancelAt.Valid {
		job.CancelRequestedAt = cancelAt.Time
	}
	if finishedAt.Valid {
		job.FinishedAt = finishedAt.Time
	}
	return job, nil
}

func (s *MySQLStore) EnqueueOperationJob(ctx context.Context, job OperationJob) (OperationJob, error) {
	scope := job.ScopeType
	if scope == "" {
		scope = OperationJobScopeTenant
	}
	switch scope {
	case OperationJobScopeTenant:
		if job.TenantID == "" {
			return OperationJob{}, errors.New("tenant-scope operation job requires a tenant")
		}
	case OperationJobScopeSystem:
		if job.TenantID != "" {
			return OperationJob{}, errors.New("system-scope operation job must not carry a tenant")
		}
	default:
		return OperationJob{}, errors.New("operation job scope must be tenant or system")
	}
	if job.JobType == "" || job.IdempotencyKey == "" || len(job.RequestHash) != 64 {
		return OperationJob{}, errors.New("operation job type, idempotency key and request hash are required")
	}
	if job.ID == "" {
		id, err := newIdentityID()
		if err != nil {
			return OperationJob{}, err
		}
		job.ID = id
	}
	checkpoint := job.CheckpointJSON
	if len(checkpoint) == 0 {
		checkpoint = json.RawMessage("{}")
	}
	// A system job stores tenant_id NULL; the generated idempotency_domain gives
	// it its own dedup namespace. The re-select uses <=> so NULL matches NULL.
	var tenantArg any
	if job.TenantID != "" {
		tenantArg = job.TenantID
	}
	var scheduleArg any
	if job.ScheduleID != "" {
		scheduleArg = job.ScheduleID
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO operation_jobs (
			id, tenant_id, scope_type, schedule_id, job_type, status, idempotency_key, request_hash,
			checkpoint_json, created_by, next_attempt_at
		) VALUES (?, ?, ?, ?, ?, 'queued', ?, ?, ?, NULLIF(?, ''), ?)
		ON DUPLICATE KEY UPDATE id = id
	`, job.ID, tenantArg, scope, scheduleArg, job.JobType, job.IdempotencyKey, job.RequestHash,
		string(checkpoint), job.CreatedBy, time.Now().UTC()); err != nil {
		return OperationJob{}, err
	}
	stored, err := scanOperationJob(s.db.QueryRowContext(ctx, `
		SELECT `+operationJobColumns+`
		FROM operation_jobs
		WHERE tenant_id <=> ? AND job_type = ? AND idempotency_key = ?
	`, tenantArg, job.JobType, job.IdempotencyKey))
	if err != nil {
		return OperationJob{}, err
	}
	if stored.RequestHash != job.RequestHash {
		return OperationJob{}, ErrOperationJobHashMismatch
	}
	return stored, nil
}

// GetSystemOperationJob reads a system-scope job by id. System jobs have no
// tenant, so they are addressed by id alone; the scope filter keeps a tenant's
// job id from resolving here.
func (s *MySQLStore) GetSystemOperationJob(ctx context.Context, jobID ID) (OperationJob, error) {
	return scanOperationJob(s.db.QueryRowContext(ctx, `
		SELECT `+operationJobColumns+`
		FROM operation_jobs
		WHERE id = ? AND scope_type = 'system'
	`, jobID))
}

func (s *MySQLStore) GetOperationJob(ctx context.Context, tenantID, jobID ID) (OperationJob, error) {
	return scanOperationJob(s.db.QueryRowContext(ctx, `
		SELECT `+operationJobColumns+`
		FROM operation_jobs
		WHERE tenant_id = ? AND id = ?
	`, tenantID, jobID))
}

// OperationJobFilter narrows and pages ListOperationJobs. Cursor is the opaque
// value returned by the previous page; jobs are ordered newest-created first.
type OperationJobFilter struct {
	JobType string
	Status  string
	Limit   int
	Cursor  string
}

func (s *MySQLStore) ListOperationJobs(ctx context.Context, tenantID ID, filter OperationJobFilter) ([]OperationJob, string, error) {
	limit := filter.Limit
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	query := `
		SELECT ` + operationJobColumns + `
		FROM operation_jobs
		WHERE tenant_id = ?`
	args := []any{tenantID}
	if filter.JobType != "" {
		query += ` AND job_type = ?`
		args = append(args, filter.JobType)
	}
	if filter.Status != "" {
		query += ` AND status = ?`
		args = append(args, filter.Status)
	}
	if filter.Cursor != "" {
		cursorTime, cursorID, err := decodeAuditCursor(filter.Cursor)
		if err != nil {
			return nil, "", err
		}
		query += ` AND (created_at < ? OR (created_at = ? AND id < ?))`
		args = append(args, cursorTime, cursorTime, cursorID)
	}
	query += ` ORDER BY created_at DESC, id DESC LIMIT ?`
	args = append(args, limit+1)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	var jobs []OperationJob
	for rows.Next() {
		job, err := scanOperationJob(rows)
		if err != nil {
			return nil, "", err
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	nextCursor := ""
	if len(jobs) > limit {
		jobs = jobs[:limit]
		last := jobs[len(jobs)-1]
		nextCursor = encodeAuditCursor(last.CreatedAt, last.ID)
	}
	return jobs, nextCursor, nil
}

func (s *MySQLStore) LeaseNextOperationJob(ctx context.Context, jobType, owner string, leaseFor time.Duration) (OperationJob, error) {
	if jobType == "" || owner == "" || len(owner) > 128 || leaseFor <= 0 {
		return OperationJob{}, errors.New("operation job lease type, owner and duration are required")
	}
	token, err := newOperationJobToken()
	if err != nil {
		return OperationJob{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return OperationJob{}, err
	}
	defer tx.Rollback()
	now := time.Now().UTC()
	var jobID ID
	err = tx.QueryRowContext(ctx, `
		SELECT id FROM operation_jobs
		WHERE job_type = ?
			AND ((status = 'queued' AND next_attempt_at <= ?)
				OR (status IN ('running', 'validating', 'cancel_requested') AND lease_expires_at <= ?))
		ORDER BY next_attempt_at
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`, jobType, now, now).Scan(&jobID)
	if err != nil {
		return OperationJob{}, err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE operation_jobs
		SET status = IF(status = 'cancel_requested', 'cancel_requested', 'running'),
			lease_owner = ?, lease_token = ?, lease_expires_at = ?,
			started_at = COALESCE(started_at, ?), heartbeat_at = ?,
			attempt_count = attempt_count + 1,
			row_version = row_version + 1, updated_at = CURRENT_TIMESTAMP(3)
		WHERE id = ?
	`, owner, token, now.Add(leaseFor), now, now, jobID); err != nil {
		return OperationJob{}, err
	}
	job, err := scanOperationJob(tx.QueryRowContext(ctx, `
		SELECT `+operationJobColumns+`
		FROM operation_jobs WHERE id = ?
	`, jobID))
	if err != nil {
		return OperationJob{}, err
	}
	if err := tx.Commit(); err != nil {
		return OperationJob{}, err
	}
	return job, nil
}

func (s *MySQLStore) HeartbeatOperationJob(ctx context.Context, jobID ID, leaseToken string, leaseFor time.Duration, progressDone uint64, checkpoint json.RawMessage) (bool, error) {
	now := time.Now().UTC()
	result, err := s.db.ExecContext(ctx, `
		UPDATE operation_jobs
		SET lease_expires_at = ?, heartbeat_at = ?, progress_done = ?,
			checkpoint_json = COALESCE(NULLIF(?, ''), checkpoint_json),
			row_version = row_version + 1, updated_at = CURRENT_TIMESTAMP(3)
		WHERE id = ? AND lease_token = ?
			AND status IN ('running', 'validating', 'cancel_requested')
	`, now.Add(leaseFor), now, progressDone, string(checkpoint), jobID, leaseToken)
	if err != nil {
		return false, err
	}
	if count, err := result.RowsAffected(); err != nil {
		return false, err
	} else if count == 0 {
		return false, ErrOperationJobLeaseLost
	}
	var status string
	if err := s.db.QueryRowContext(ctx, `SELECT status FROM operation_jobs WHERE id = ?`, jobID).Scan(&status); err != nil {
		return false, err
	}
	return status == OperationJobStatusCancelRequested, nil
}

func (s *MySQLStore) CompleteOperationJobSucceeded(ctx context.Context, jobID ID, leaseToken, resultRef string) error {
	return s.finishOperationJob(ctx, jobID, leaseToken, OperationJobStatusSucceeded, resultRef, "", "")
}

func (s *MySQLStore) CompleteOperationJobCanceled(ctx context.Context, jobID ID, leaseToken string) error {
	return s.finishOperationJob(ctx, jobID, leaseToken, OperationJobStatusCanceled, "", "", "")
}

func (s *MySQLStore) CompleteOperationJobFailed(ctx context.Context, jobID ID, leaseToken, errorCode, errorDetail string, retry bool, retryAt time.Time) error {
	if !retry {
		return s.finishOperationJob(ctx, jobID, leaseToken, OperationJobStatusFailed, "", errorCode, errorDetail)
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE operation_jobs
		SET status = 'queued', lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
			next_attempt_at = ?, last_error_code = NULLIF(?, ''), last_error_detail = NULLIF(LEFT(?, 1024), ''),
			row_version = row_version + 1, updated_at = CURRENT_TIMESTAMP(3)
		WHERE id = ? AND lease_token = ?
			AND status IN ('running', 'validating', 'cancel_requested')
	`, retryAt.UTC(), errorCode, errorDetail, jobID, leaseToken)
	return operationJobWriteOutcome(result, err)
}

func (s *MySQLStore) finishOperationJob(ctx context.Context, jobID ID, leaseToken, status, resultRef, errorCode, errorDetail string) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE operation_jobs
		SET status = ?, result_ref = NULLIF(?, ''),
			last_error_code = NULLIF(?, ''), last_error_detail = NULLIF(LEFT(?, 1024), ''),
			lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
			finished_at = ?, row_version = row_version + 1, updated_at = CURRENT_TIMESTAMP(3)
		WHERE id = ? AND lease_token = ?
			AND status IN ('running', 'validating', 'cancel_requested')
	`, status, resultRef, errorCode, errorDetail, time.Now().UTC(), jobID, leaseToken)
	return operationJobWriteOutcome(result, err)
}

func operationJobWriteOutcome(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		return ErrOperationJobLeaseLost
	}
	return nil
}

// RequestOperationJobCancel cancels a queued job immediately and flags a
// running one for its worker to stop; canceling an already-terminal or
// already-flagged job is an idempotent no-op.
func (s *MySQLStore) RequestOperationJobCancel(ctx context.Context, tenantID, jobID ID) error {
	now := time.Now().UTC()
	result, err := s.db.ExecContext(ctx, `
		UPDATE operation_jobs
		SET status = 'canceled', cancel_requested_at = ?, finished_at = ?,
			row_version = row_version + 1, updated_at = CURRENT_TIMESTAMP(3)
		WHERE tenant_id = ? AND id = ? AND status = 'queued'
	`, now, now, tenantID, jobID)
	if err != nil {
		return err
	}
	if count, err := result.RowsAffected(); err != nil {
		return err
	} else if count > 0 {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, `
		UPDATE operation_jobs
		SET status = 'cancel_requested', cancel_requested_at = COALESCE(cancel_requested_at, ?),
			row_version = row_version + 1, updated_at = CURRENT_TIMESTAMP(3)
		WHERE tenant_id = ? AND id = ? AND status IN ('running', 'validating')
	`, now, tenantID, jobID); err != nil {
		return err
	}
	var exists int
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM operation_jobs WHERE tenant_id = ? AND id = ?
	`, tenantID, jobID).Scan(&exists); err != nil {
		return err
	}
	if exists == 0 {
		return sql.ErrNoRows
	}
	return nil
}
