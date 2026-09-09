// Package opjob is the watchdog v2 async job engine: a de-tenanted port of the
// SaaS operation_jobs framework. Enqueue is idempotent per (job_type,
// idempotency_key); execution is claimed with a lease (SELECT ... FOR UPDATE
// SKIP LOCKED) so exactly one worker runs a job at a time and a crashed worker's
// job is taken over after its lease expires; every attempt heartbeats, observes
// cancellation, and finishes as succeeded / retried / terminally failed /
// canceled. It backs the address import/publish/GC jobs; job_type discriminates.
package opjob

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base32"
	"encoding/hex"
	"encoding/json"
	"errors"
	"time"
)

// jobTable is the installation's only asynchronous operation state machine.
// Migration 0014 retires the incomplete baseline draft and renames the complete
// lease/retry/checkpoint engine created by 0010 to this canonical table name.
const jobTable = "operation_jobs"

var (
	ErrLeaseLost    = errors.New("operation job lease is no longer held")
	ErrHashMismatch = errors.New("idempotency key was already used with a different request")
)

const (
	StatusQueued          = "queued"
	StatusRunning         = "running"
	StatusCancelRequested = "cancel_requested"
	StatusSucceeded       = "succeeded"
	StatusFailed          = "failed"
	StatusCanceled        = "canceled"
)

// CodeTerminal is the failure code recorded when a handler error is
// non-retryable, as distinct from a retryable error that exhausted its budget.
const CodeTerminal = "TERMINAL"

// Job is one async job row.
type Job struct {
	ID                string          `json:"id"`
	JobType           string          `json:"job_type"`
	Status            string          `json:"status"`
	IdempotencyKey    string          `json:"idempotency_key"`
	RequestHash       string          `json:"request_hash"`
	ProgressTotal     uint64          `json:"progress_total"`
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
	CreatedBy         string          `json:"created_by,omitempty"`
	CreatedAt         time.Time       `json:"created_at"`
	StartedAt         time.Time       `json:"started_at,omitzero"`
	CancelRequestedAt time.Time       `json:"cancel_requested_at,omitzero"`
	FinishedAt        time.Time       `json:"finished_at,omitzero"`
}

func (j Job) Terminal() bool {
	switch j.Status {
	case StatusSucceeded, StatusFailed, StatusCanceled:
		return true
	}
	return false
}

// Repository is the persistence contract the worker drives.
type Repository interface {
	Enqueue(ctx context.Context, job Job) (Job, error)
	Get(ctx context.Context, jobID string) (Job, error)
	List(ctx context.Context, filter Filter) ([]Job, error)
	// LeaseNext claims the next due job of the type: a queued job whose
	// next_attempt_at has passed, or a running job whose lease expired
	// (takeover). sql.ErrNoRows when nothing is due.
	LeaseNext(ctx context.Context, jobType, owner string, leaseFor time.Duration) (Job, error)
	// Heartbeat extends the lease and reports whether cancellation was
	// requested. ErrLeaseLost when the token no longer holds.
	Heartbeat(ctx context.Context, jobID, leaseToken string, leaseFor time.Duration, progressDone uint64, checkpoint json.RawMessage) (cancelRequested bool, err error)
	CompleteSucceeded(ctx context.Context, jobID, leaseToken, resultRef string) error
	CompleteCanceled(ctx context.Context, jobID, leaseToken string) error
	CompleteFailed(ctx context.Context, jobID, leaseToken, errorCode, errorDetail string, retry bool, retryAt time.Time) error
	RequestCancel(ctx context.Context, jobID string) error
}

// Filter narrows List. Jobs are ordered newest-created first.
type Filter struct {
	JobType   string
	Status    string
	CreatedBy string
	Search    string
	Ascending bool
	Limit     int
	Offset    int
}

// Store is the *sql.DB-backed Repository.
type Store struct{ db *sql.DB }

func NewStore(db *sql.DB) *Store { return &Store{db: db} }

var crockford = base32.NewEncoding("0123456789ABCDEFGHJKMNPQRSTVWXYZ").WithPadding(base32.NoPadding)

func newJobID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("crypto/rand failed: " + err.Error())
	}
	return crockford.EncodeToString(b[:])
}

func newLeaseToken() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

type rowScanner interface{ Scan(dest ...any) error }

const jobColumns = `
	id, job_type, status, idempotency_key, request_hash,
	progress_total, progress_done, checkpoint_json, COALESCE(result_ref, ''),
	COALESCE(lease_owner, ''), COALESCE(lease_token, ''), lease_expires_at,
	next_attempt_at, attempt_count, COALESCE(last_error_code, ''), COALESCE(last_error_detail, ''),
	row_version, COALESCE(created_by, ''), created_at, started_at, cancel_requested_at, finished_at`

func scanJob(row rowScanner) (Job, error) {
	var job Job
	var leaseExpires, startedAt, cancelAt, finishedAt sql.NullTime
	var checkpoint []byte
	err := row.Scan(&job.ID, &job.JobType, &job.Status, &job.IdempotencyKey, &job.RequestHash,
		&job.ProgressTotal, &job.ProgressDone, &checkpoint, &job.ResultRef,
		&job.LeaseOwner, &job.LeaseToken, &leaseExpires,
		&job.NextAttemptAt, &job.AttemptCount, &job.LastErrorCode, &job.LastErrorDetail,
		&job.RowVersion, &job.CreatedBy, &job.CreatedAt, &startedAt, &cancelAt, &finishedAt)
	if err != nil {
		return Job{}, err
	}
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

func (s *Store) Enqueue(ctx context.Context, job Job) (Job, error) {
	if job.JobType == "" || job.IdempotencyKey == "" || len(job.RequestHash) != 64 {
		return Job{}, errors.New("operation job type, idempotency key and request hash are required")
	}
	if job.ID == "" {
		job.ID = newJobID()
	}
	checkpoint := job.CheckpointJSON
	if len(checkpoint) == 0 {
		checkpoint = json.RawMessage("{}")
	}
	var createdBy any
	if job.CreatedBy != "" {
		createdBy = job.CreatedBy
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO `+jobTable+` (
			id, job_type, status, idempotency_key, request_hash,
			progress_total, checkpoint_json, created_by, next_attempt_at
		) VALUES (?, ?, 'queued', ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE id = id
	`, job.ID, job.JobType, job.IdempotencyKey, job.RequestHash, job.ProgressTotal,
		string(checkpoint), createdBy, time.Now().UTC()); err != nil {
		return Job{}, err
	}
	stored, err := scanJob(s.db.QueryRowContext(ctx, `
		SELECT `+jobColumns+` FROM `+jobTable+`
		WHERE job_type = ? AND idempotency_key = ?
	`, job.JobType, job.IdempotencyKey))
	if err != nil {
		return Job{}, err
	}
	if stored.RequestHash != job.RequestHash {
		return Job{}, ErrHashMismatch
	}
	return stored, nil
}

func (s *Store) Get(ctx context.Context, jobID string) (Job, error) {
	return scanJob(s.db.QueryRowContext(ctx,
		`SELECT `+jobColumns+` FROM `+jobTable+` WHERE id = ?`, jobID))
}

func (s *Store) List(ctx context.Context, filter Filter) ([]Job, error) {
	limit := filter.Limit
	if limit <= 0 {
		limit = 50
	}
	if limit > 200 {
		limit = 200
	}
	query := `SELECT ` + jobColumns + ` FROM ` + jobTable + ` WHERE 1=1`
	args := []any{}
	if filter.JobType != "" {
		query += ` AND job_type = ?`
		args = append(args, filter.JobType)
	}
	if filter.Status != "" {
		query += ` AND status = ?`
		args = append(args, filter.Status)
	}
	if filter.CreatedBy != "" {
		query += ` AND created_by = ?`
		args = append(args, filter.CreatedBy)
	}
	if filter.Search != "" {
		query += ` AND (LOCATE(?, id)>0 OR LOCATE(?, result_ref)>0)`
		args = append(args, filter.Search, filter.Search)
	}
	direction := "DESC"
	if filter.Ascending {
		direction = "ASC"
	}
	query += ` ORDER BY created_at ` + direction + `, id ` + direction + ` LIMIT ? OFFSET ?`
	args = append(args, limit, filter.Offset)
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var jobs []Job
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

// Count applies the same ownership/type/status predicates as List and is used
// by paginated management APIs. It deliberately does not expose free-form SQL
// search or sort over the shared operation state machine.
func (s *Store) Count(ctx context.Context, filter Filter) (uint64, error) {
	query := `SELECT COUNT(*) FROM ` + jobTable + ` WHERE 1=1`
	args := []any{}
	if filter.JobType != "" {
		query += ` AND job_type = ?`
		args = append(args, filter.JobType)
	}
	if filter.Status != "" {
		query += ` AND status = ?`
		args = append(args, filter.Status)
	}
	if filter.CreatedBy != "" {
		query += ` AND created_by = ?`
		args = append(args, filter.CreatedBy)
	}
	if filter.Search != "" {
		query += ` AND (LOCATE(?, id)>0 OR LOCATE(?, result_ref)>0)`
		args = append(args, filter.Search, filter.Search)
	}
	var total uint64
	err := s.db.QueryRowContext(ctx, query, args...).Scan(&total)
	return total, err
}

func (s *Store) LeaseNext(ctx context.Context, jobType, owner string, leaseFor time.Duration) (Job, error) {
	if jobType == "" || owner == "" || len(owner) > 128 || leaseFor <= 0 {
		return Job{}, errors.New("operation job lease type, owner and duration are required")
	}
	token, err := newLeaseToken()
	if err != nil {
		return Job{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Job{}, err
	}
	defer tx.Rollback()
	now := time.Now().UTC()
	var jobID string
	err = tx.QueryRowContext(ctx, `
		SELECT id FROM `+jobTable+`
		WHERE job_type = ?
			AND ((status = 'queued' AND next_attempt_at <= ?)
				OR (status IN ('running', 'cancel_requested') AND lease_expires_at <= ?))
		ORDER BY next_attempt_at
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`, jobType, now, now).Scan(&jobID)
	if err != nil {
		return Job{}, err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE `+jobTable+`
		SET status = IF(status = 'cancel_requested', 'cancel_requested', 'running'),
			lease_owner = ?, lease_token = ?, lease_expires_at = ?,
			started_at = COALESCE(started_at, ?), heartbeat_at = ?,
			attempt_count = attempt_count + 1,
			row_version = row_version + 1, updated_at = CURRENT_TIMESTAMP(3)
		WHERE id = ?
	`, owner, token, now.Add(leaseFor), now, now, jobID); err != nil {
		return Job{}, err
	}
	job, err := scanJob(tx.QueryRowContext(ctx,
		`SELECT `+jobColumns+` FROM `+jobTable+` WHERE id = ?`, jobID))
	if err != nil {
		return Job{}, err
	}
	if err := tx.Commit(); err != nil {
		return Job{}, err
	}
	return job, nil
}

func (s *Store) Heartbeat(ctx context.Context, jobID, leaseToken string, leaseFor time.Duration, progressDone uint64, checkpoint json.RawMessage) (bool, error) {
	now := time.Now().UTC()
	result, err := s.db.ExecContext(ctx, `
		UPDATE `+jobTable+`
		SET lease_expires_at = ?, heartbeat_at = ?, progress_done = ?,
			checkpoint_json = COALESCE(NULLIF(?, ''), checkpoint_json),
			row_version = row_version + 1, updated_at = CURRENT_TIMESTAMP(3)
		WHERE id = ? AND lease_token = ?
			AND status IN ('running', 'cancel_requested')
	`, now.Add(leaseFor), now, progressDone, string(checkpoint), jobID, leaseToken)
	if err != nil {
		return false, err
	}
	if count, err := result.RowsAffected(); err != nil {
		return false, err
	} else if count == 0 {
		return false, ErrLeaseLost
	}
	var status string
	if err := s.db.QueryRowContext(ctx, `SELECT status FROM `+jobTable+` WHERE id = ?`, jobID).Scan(&status); err != nil {
		return false, err
	}
	return status == StatusCancelRequested, nil
}

func (s *Store) CompleteSucceeded(ctx context.Context, jobID, leaseToken, resultRef string) error {
	return s.finish(ctx, jobID, leaseToken, StatusSucceeded, resultRef, "", "")
}

func (s *Store) CompleteCanceled(ctx context.Context, jobID, leaseToken string) error {
	return s.finish(ctx, jobID, leaseToken, StatusCanceled, "", "", "")
}

func (s *Store) CompleteFailed(ctx context.Context, jobID, leaseToken, errorCode, errorDetail string, retry bool, retryAt time.Time) error {
	if !retry {
		return s.finish(ctx, jobID, leaseToken, StatusFailed, "", errorCode, errorDetail)
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE `+jobTable+`
		SET status = 'queued', lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
			next_attempt_at = ?, last_error_code = NULLIF(?, ''), last_error_detail = NULLIF(LEFT(?, 1024), ''),
			row_version = row_version + 1, updated_at = CURRENT_TIMESTAMP(3)
		WHERE id = ? AND lease_token = ?
			AND status IN ('running', 'cancel_requested')
	`, retryAt.UTC(), errorCode, errorDetail, jobID, leaseToken)
	return writeOutcome(result, err)
}

func (s *Store) finish(ctx context.Context, jobID, leaseToken, status, resultRef, errorCode, errorDetail string) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE `+jobTable+`
		SET status = ?, result_ref = NULLIF(?, ''),
			last_error_code = NULLIF(?, ''), last_error_detail = NULLIF(LEFT(?, 1024), ''),
			lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
			finished_at = ?, row_version = row_version + 1, updated_at = CURRENT_TIMESTAMP(3)
		WHERE id = ? AND lease_token = ?
			AND status IN ('running', 'cancel_requested')
	`, status, resultRef, errorCode, errorDetail, time.Now().UTC(), jobID, leaseToken)
	return writeOutcome(result, err)
}

func writeOutcome(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		return ErrLeaseLost
	}
	return nil
}

// RequestCancel cancels a queued job immediately and flags a running one for its
// worker to stop; canceling an already-terminal or already-flagged job is an
// idempotent no-op. sql.ErrNoRows when the job does not exist.
func (s *Store) RequestCancel(ctx context.Context, jobID string) error {
	now := time.Now().UTC()
	result, err := s.db.ExecContext(ctx, `
		UPDATE `+jobTable+`
		SET status = 'canceled', cancel_requested_at = ?, finished_at = ?,
			row_version = row_version + 1, updated_at = CURRENT_TIMESTAMP(3)
		WHERE id = ? AND status = 'queued'
	`, now, now, jobID)
	if err != nil {
		return err
	}
	if count, err := result.RowsAffected(); err != nil {
		return err
	} else if count > 0 {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, `
		UPDATE `+jobTable+`
		SET status = 'cancel_requested', cancel_requested_at = COALESCE(cancel_requested_at, ?),
			row_version = row_version + 1, updated_at = CURRENT_TIMESTAMP(3)
		WHERE id = ? AND status = 'running'
	`, now, jobID); err != nil {
		return err
	}
	var exists int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM `+jobTable+` WHERE id = ?`, jobID).Scan(&exists); err != nil {
		return err
	}
	if exists == 0 {
		return sql.ErrNoRows
	}
	return nil
}

var _ Repository = (*Store)(nil)
