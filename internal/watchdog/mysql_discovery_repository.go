package watchdog

import (
	"context"
	"database/sql"
)

func (s *MySQLStore) EnqueueDiscoveryJob(ctx context.Context, tenantID, deviceID ID, reason string) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO discovery_jobs (id, tenant_id, device_id, reason, status, due_at)
		VALUES (?, ?, ?, ?, 'pending', CURRENT_TIMESTAMP(3))
		ON DUPLICATE KEY UPDATE
			reason = VALUES(reason),
			status = 'pending',
			due_at = CURRENT_TIMESTAMP(3),
			started_at = NULL,
			completed_at = NULL,
			last_error = NULL
	`, collectorStableID("discovery-job", string(tenantID), string(deviceID)), tenantID, deviceID, reason)
	return err
}

func (s *MySQLStore) ListDueDiscoveryJobs(ctx context.Context, tenantID ID, limit int) ([]DiscoveryJob, error) {
	if limit <= 0 {
		limit = 10
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, tenant_id, device_id, reason, status, due_at, started_at, completed_at, last_error, created_at, updated_at
		FROM discovery_jobs
		WHERE tenant_id = ? AND status = 'pending' AND due_at <= CURRENT_TIMESTAMP(3)
		ORDER BY due_at ASC
		LIMIT ?
	`, tenantID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var jobs []DiscoveryJob
	for rows.Next() {
		var job DiscoveryJob
		var startedAt, completedAt sql.NullTime
		var lastError sql.NullString
		if err := rows.Scan(&job.ID, &job.TenantID, &job.DeviceID, &job.Reason, &job.Status, &job.DueAt, &startedAt, &completedAt, &lastError, &job.CreatedAt, &job.UpdatedAt); err != nil {
			return nil, err
		}
		job.StartedAt = startedAt.Time
		job.CompletedAt = completedAt.Time
		job.LastError = lastError.String
		jobs = append(jobs, job)
	}
	return jobs, rows.Err()
}

func (s *MySQLStore) MarkDiscoveryJobRunning(ctx context.Context, jobID ID) (bool, error) {
	result, err := s.db.ExecContext(ctx, `
		UPDATE discovery_jobs SET status = 'running', started_at = CURRENT_TIMESTAMP(3) WHERE id = ? AND status = 'pending'
	`, jobID)
	if err != nil {
		return false, err
	}
	affected, err := result.RowsAffected()
	return affected == 1, err
}

func (s *MySQLStore) MarkDiscoveryJobCompleted(ctx context.Context, jobID ID, lastError string) error {
	status := discoveryJobCompleted
	if lastError != "" {
		status = discoveryJobFailed
	}
	_, err := s.db.ExecContext(ctx, `
		UPDATE discovery_jobs
		SET status = ?, completed_at = CURRENT_TIMESTAMP(3), last_error = NULLIF(?, '')
		WHERE id = ? AND status = 'running'
	`, status, lastError, jobID)
	return err
}
