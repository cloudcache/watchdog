package watchdog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

func (s *MySQLStore) CreateExportTask(ctx context.Context, task ExportTask) (ExportTask, error) {
	task = normalizeExportTask(task)
	if task.ID == "" {
		return ExportTask{}, errors.New("export task id is required")
	}
	var err error
	if task.ContractVersion == 0 {
		task, err = normalizeLegacyExportTaskSnapshot(task)
	} else {
		err = validateExportExecutionTask(task)
	}
	if err != nil {
		return ExportTask{}, err
	}
	if task.ContractVersion == ExportExecutionContractVersion {
		return s.createExportExecution(ctx, task)
	}
	if err := insertExportTask(ctx, s.db, task); err != nil {
		return ExportTask{}, err
	}
	return s.GetExportTask(ctx, task.TenantID, task.ID)
}

type exportTaskExecer interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
}

func insertExportTask(ctx context.Context, exec exportTaskExecer, task ExportTask) error {
	_, err := exec.ExecContext(ctx, `
		INSERT INTO export_tasks (
			id, tenant_id, created_by, contract_version, dataset_key, query_json, query_hash,
			value_layer, versions_json, authorization_json, operation_job_id, retention_seconds,
			target_id, port_id, period_type, range_start, range_end, step_seconds,
			aggregation, value_mode, format, status
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULLIF(?, ''), ?,
			NULLIF(?, ''), NULLIF(?, ''), ?, ?, ?, ?, ?, ?, ?, ?)
	`, task.ID, task.TenantID, task.CreatedBy, task.ContractVersion, task.DatasetKey,
		string(task.QueryJSON), task.QueryHash, task.ValueLayer, string(task.VersionsJSON),
		string(task.AuthorizationJSON), task.OperationJobID, task.RetentionSeconds,
		task.TargetID, task.PortID, task.PeriodType, task.RangeStart, task.RangeEnd,
		uint32(task.Step.Seconds()), task.Aggregation, task.ValueMode, task.Format, task.Status)
	return err
}

func (s *MySQLStore) createExportExecution(ctx context.Context, task ExportTask) (ExportTask, error) {
	checkpoint, requestHash, err := encodeExportExecutionPayload(task)
	if err != nil {
		return ExportTask{}, err
	}
	jobID, err := newIdentityID()
	if err != nil {
		return ExportTask{}, err
	}
	task.OperationJobID = jobID
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return ExportTask{}, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO operation_jobs (
			id, tenant_id, scope_type, job_type, status, idempotency_key,
			request_hash, checkpoint_json, created_by, next_attempt_at
		) VALUES (?, ?, 'tenant', ?, 'queued', ?, ?, ?, NULLIF(?, ''), ?)
	`, jobID, task.TenantID, ExportExecutionJobType, "export:"+string(task.ID), requestHash,
		string(checkpoint), task.CreatedBy, time.Now().UTC()); err != nil {
		return ExportTask{}, err
	}
	if err := insertExportTask(ctx, tx, task); err != nil {
		return ExportTask{}, err
	}
	if err := tx.Commit(); err != nil {
		return ExportTask{}, err
	}
	return s.GetExportTask(ctx, task.TenantID, task.ID)
}

func (s *MySQLStore) GetExportTask(ctx context.Context, tenantID, taskID ID) (ExportTask, error) {
	row := s.db.QueryRowContext(ctx, exportTaskSelect()+`
		WHERE e.tenant_id = ? AND e.id = ?
	`, tenantID, taskID)
	return scanExportTask(row)
}

func (s *MySQLStore) ListExportTasks(ctx context.Context, tenantID ID, createdBy ID) ([]ExportTask, error) {
	query := exportTaskSelect() + ` WHERE e.tenant_id = ?`
	args := []any{tenantID}
	if createdBy != "" {
		query += ` AND e.created_by = ?`
		args = append(args, createdBy)
	}
	query += ` ORDER BY e.created_at DESC`
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tasks []ExportTask
	for rows.Next() {
		task, err := scanExportTask(rows)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	return tasks, rows.Err()
}

func (s *MySQLStore) ListPendingExportTasks(ctx context.Context, limit int) ([]ExportTask, error) {
	if limit <= 0 {
		limit = 10
	}
	rows, err := s.db.QueryContext(ctx, exportTaskSelect()+`
		WHERE e.contract_version = 0 AND e.status = ?
		ORDER BY e.created_at
		LIMIT ?
	`, ExportStatusPending, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var tasks []ExportTask
	for rows.Next() {
		task, err := scanExportTask(rows)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, task)
	}
	return tasks, rows.Err()
}

func (s *MySQLStore) MarkExportRunning(ctx context.Context, tenantID, taskID ID) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE export_tasks
		SET status = ?, error_message = NULL, row_version = row_version + 1,
			updated_at = CURRENT_TIMESTAMP(3)
		WHERE tenant_id = ? AND id = ?
	`, ExportStatusRunning, tenantID, taskID)
	return requireOneExportRow(result, err)
}

func (s *MySQLStore) RetryExportTask(ctx context.Context, tenantID, taskID ID) error {
	var contractVersion uint16
	if err := s.db.QueryRowContext(ctx, `SELECT contract_version FROM export_tasks WHERE tenant_id = ? AND id = ?`, tenantID, taskID).Scan(&contractVersion); err != nil {
		return err
	}
	if contractVersion == ExportExecutionContractVersion {
		return s.retryExportExecution(ctx, tenantID, taskID)
	}
	result, err := s.db.ExecContext(ctx, `
		UPDATE export_tasks
		SET status = ?, file_ref = NULL, checksum = NULL, size_bytes = NULL,
			expires_at = NULL, error_message = NULL, row_version = row_version + 1,
			updated_at = CURRENT_TIMESTAMP(3)
		WHERE tenant_id = ? AND id = ? AND status = ?
	`, ExportStatusPending, tenantID, taskID, ExportStatusFailed)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return sql.ErrNoRows
	}
	return nil
}

func (s *MySQLStore) MarkExportComplete(ctx context.Context, tenantID, taskID ID, artifact ExportArtifact, expiresAt time.Time) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE export_tasks
		SET status = ?, file_ref = ?, checksum = ?, size_bytes = ?, expires_at = ?,
			artifact_schema_version = ?, content_type = ?, row_count = ?,
			error_message = NULL, row_version = row_version + 1, updated_at = CURRENT_TIMESTAMP(3)
		WHERE tenant_id = ? AND id = ?
	`, ExportStatusComplete, artifact.FileRef, artifact.Checksum, artifact.SizeBytes, expiresAt.UTC(),
		artifact.SchemaVersion, artifact.ContentType, artifact.RowCount, tenantID, taskID)
	return requireOneExportRow(result, err)
}

func (s *MySQLStore) MarkExportFailed(ctx context.Context, tenantID, taskID ID, message string) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE export_tasks
		SET status = ?, error_message = ?, row_version = row_version + 1, updated_at = CURRENT_TIMESTAMP(3)
		WHERE tenant_id = ? AND id = ?
	`, ExportStatusFailed, message, tenantID, taskID)
	return requireOneExportRow(result, err)
}

func normalizeExportTask(task ExportTask) ExportTask {
	if task.PeriodType == "" {
		task.PeriodType = PeriodCustom
	}
	if task.Step == 0 {
		task.Step = 5 * time.Minute
	}
	if task.ValueMode == "" {
		task.ValueMode = ExportValueCorrected
	}
	if task.Format == "" {
		task.Format = ExportFormatCSV
	}
	if task.Status == "" {
		task.Status = ExportStatusPending
	}
	if task.ValueLayer == "" {
		task.ValueLayer = inferExportValueLayer(task)
	}
	if task.DatasetKey == "" {
		task.DatasetKey = defaultExportDatasetKey
	}
	if task.RetentionSeconds == 0 {
		task.RetentionSeconds = defaultExportRetentionSeconds
	}
	return task
}

func exportTaskSelect() string {
	return `
		SELECT e.id, e.tenant_id, e.created_by, e.contract_version, e.dataset_key,
		       e.query_json, e.query_hash, e.value_layer, e.versions_json,
		       e.authorization_json, COALESCE(e.operation_job_id, ''), e.retention_seconds,
		       COALESCE(e.artifact_schema_version, 0), COALESCE(e.content_type, ''),
		       COALESCE(e.row_count, 0), COALESCE(e.target_id, ''), COALESCE(e.port_id, ''),
		       e.period_type, e.range_start, e.range_end, e.step_seconds, e.aggregation, e.value_mode,
		       e.format,
		       CASE WHEN e.contract_version = 1 THEN
		         CASE oj.status
		           WHEN 'queued' THEN 'pending'
		           WHEN 'running' THEN 'running'
		           WHEN 'cancel_requested' THEN 'running'
		           WHEN 'succeeded' THEN 'complete'
		           WHEN 'failed' THEN 'failed'
		           WHEN 'canceled' THEN 'canceled'
		           ELSE e.status
		         END
		       ELSE e.status END,
		       COALESCE(e.file_ref, ''), COALESCE(e.checksum, ''), COALESCE(e.size_bytes, 0), e.expires_at,
		       CASE WHEN e.contract_version = 1 AND oj.last_error_detail IS NOT NULL
		         THEN oj.last_error_detail ELSE COALESCE(e.error_message, '') END,
		       e.row_version, e.created_at, e.updated_at
		FROM export_tasks e
		LEFT JOIN operation_jobs oj ON oj.id = e.operation_job_id
	`
}

func scanExportTask(row rowScanner) (ExportTask, error) {
	var task ExportTask
	var stepSeconds uint32
	var expiresAt sql.NullTime
	var queryJSON, versionsJSON, authorizationJSON []byte
	err := row.Scan(
		&task.ID,
		&task.TenantID,
		&task.CreatedBy,
		&task.ContractVersion,
		&task.DatasetKey,
		&queryJSON,
		&task.QueryHash,
		&task.ValueLayer,
		&versionsJSON,
		&authorizationJSON,
		&task.OperationJobID,
		&task.RetentionSeconds,
		&task.ArtifactSchemaVersion,
		&task.ContentType,
		&task.RowCount,
		&task.TargetID,
		&task.PortID,
		&task.PeriodType,
		&task.RangeStart,
		&task.RangeEnd,
		&stepSeconds,
		&task.Aggregation,
		&task.ValueMode,
		&task.Format,
		&task.Status,
		&task.FileRef,
		&task.Checksum,
		&task.SizeBytes,
		&expiresAt,
		&task.ErrorMessage,
		&task.RowVersion,
		&task.CreatedAt,
		&task.UpdatedAt,
	)
	if err != nil {
		return task, err
	}
	if expiresAt.Valid {
		task.ExpiresAt = expiresAt.Time
	}
	task.Step = time.Duration(stepSeconds) * time.Second
	task.QueryJSON = json.RawMessage(queryJSON)
	task.VersionsJSON = json.RawMessage(versionsJSON)
	task.AuthorizationJSON = json.RawMessage(authorizationJSON)
	return task, nil
}

func (s *MySQLStore) retryExportExecution(ctx context.Context, tenantID, taskID ID) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var task ExportTask
	var currentJobID ID
	var queryJSON, versionsJSON, authorizationJSON []byte
	if err := tx.QueryRowContext(ctx, `
		SELECT contract_version, dataset_key, query_json, query_hash, value_layer,
		       versions_json, authorization_json, format, retention_seconds, created_by,
		       row_version, COALESCE(operation_job_id, '')
		FROM export_tasks WHERE tenant_id = ? AND id = ? FOR UPDATE
	`, tenantID, taskID).Scan(
		&task.ContractVersion, &task.DatasetKey, &queryJSON, &task.QueryHash, &task.ValueLayer,
		&versionsJSON, &authorizationJSON, &task.Format, &task.RetentionSeconds, &task.CreatedBy,
		&task.RowVersion, &currentJobID,
	); err != nil {
		return err
	}
	task.QueryJSON = json.RawMessage(queryJSON)
	task.VersionsJSON = json.RawMessage(versionsJSON)
	task.AuthorizationJSON = json.RawMessage(authorizationJSON)
	var jobStatus string
	if currentJobID == "" {
		return errors.New("export execution has no operation job")
	}
	if err := tx.QueryRowContext(ctx, `SELECT status FROM operation_jobs WHERE tenant_id = ? AND id = ?`, tenantID, currentJobID).Scan(&jobStatus); err != nil {
		return err
	}
	if jobStatus != OperationJobStatusFailed && jobStatus != OperationJobStatusCanceled {
		return errors.New("only failed or canceled export execution can be retried")
	}
	task.ID, task.TenantID = taskID, tenantID
	checkpoint, requestHash, err := encodeExportExecutionPayload(task)
	if err != nil {
		return err
	}
	newJobID, err := newIdentityID()
	if err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO operation_jobs (
			id, tenant_id, scope_type, job_type, status, idempotency_key,
			request_hash, checkpoint_json, created_by, next_attempt_at
		) VALUES (?, ?, 'tenant', ?, 'queued', ?, ?, ?, NULLIF(?, ''), ?)
	`, newJobID, tenantID, ExportExecutionJobType,
		fmt.Sprintf("export:%s:retry:%d", taskID, task.RowVersion+1), requestHash,
		string(checkpoint), task.CreatedBy, time.Now().UTC()); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE export_tasks SET operation_job_id = ?, status = 'pending', file_ref = NULL,
			checksum = NULL, size_bytes = NULL, expires_at = NULL,
			artifact_schema_version = NULL, content_type = NULL, row_count = NULL,
			error_message = NULL, row_version = row_version + 1, updated_at = CURRENT_TIMESTAMP(3)
		WHERE tenant_id = ? AND id = ? AND row_version = ?
	`, newJobID, tenantID, taskID, task.RowVersion)
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return sql.ErrNoRows
	}
	return tx.Commit()
}

func requireOneExportRow(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return sql.ErrNoRows
	}
	return nil
}

var _ ExportRepository = (*MySQLStore)(nil)
var _ PendingExportRepository = (*MySQLStore)(nil)
var _ = sql.ErrNoRows
