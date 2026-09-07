package watchdog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

var ErrExportNotTerminal = errors.New("only terminal exports can be deleted")

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

func (s *MySQLStore) ListExportTasksPage(ctx context.Context, tenantID ID, filter ExportTaskListFilter) ([]ExportTask, int64, error) {
	if err := validateExportTaskListFilter(&filter); err != nil {
		return nil, 0, err
	}
	where, args := exportTaskListWhere(tenantID, filter)
	var total int64
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM export_tasks e
		LEFT JOIN operation_jobs oj ON oj.id = e.operation_job_id
		WHERE `+where, args...).Scan(&total); err != nil {
		return nil, 0, err
	}
	sortColumn := map[string]string{
		"created_at": "e.created_at", "updated_at": "e.updated_at", "range_start": "e.range_start",
		"target_id": "e.target_id", "value_layer": "e.value_layer", "format": "e.format",
		"status": exportTaskStatusExpression(),
	}[filter.SortBy]
	queryArgs := append(append([]any(nil), args...), filter.Limit, filter.Offset)
	rows, err := s.db.QueryContext(ctx, exportTaskSelect()+` WHERE `+where+`
		ORDER BY `+sortColumn+` `+filter.SortDirection+`, e.id `+filter.SortDirection+`
		LIMIT ? OFFSET ?`, queryArgs...)
	if err != nil {
		return nil, 0, err
	}
	defer rows.Close()
	tasks := make([]ExportTask, 0, filter.Limit)
	for rows.Next() {
		task, err := scanExportTask(rows)
		if err != nil {
			return nil, 0, err
		}
		tasks = append(tasks, task)
	}
	return tasks, total, rows.Err()
}

func validateExportTaskListFilter(filter *ExportTaskListFilter) error {
	if filter.Limit == 0 {
		filter.Limit = 25
	}
	if filter.Limit < 1 || filter.Limit > 100 || filter.Offset < 0 || filter.Offset > 10_000_000 {
		return errors.New("export list limit must be 1..100 and offset must be 0..10000000")
	}
	filter.Search = strings.TrimSpace(filter.Search)
	if len(filter.Search) > 200 {
		return errors.New("export list search is too long")
	}
	if filter.Status != "" && filter.Status != ExportStatusPending && filter.Status != ExportStatusRunning &&
		filter.Status != ExportStatusComplete && filter.Status != ExportStatusFailed && filter.Status != ExportStatusCanceled {
		return errors.New("export list status is invalid")
	}
	if filter.ValueLayer != "" && filter.ValueLayer != QueryValueRaw && filter.ValueLayer != QueryValueSupplier && filter.ValueLayer != QueryValueCustomer {
		return errors.New("export list value_layer is invalid")
	}
	if filter.Format != "" && filter.Format != ExportFormatCSV && filter.Format != ExportFormatParquet {
		return errors.New("export list format is invalid")
	}
	if filter.SortBy == "" {
		filter.SortBy = "created_at"
	}
	if _, ok := map[string]struct{}{"created_at": {}, "updated_at": {}, "range_start": {}, "target_id": {}, "value_layer": {}, "format": {}, "status": {}}[filter.SortBy]; !ok {
		return errors.New("export list sort_by is invalid")
	}
	filter.SortDirection = strings.ToUpper(strings.TrimSpace(filter.SortDirection))
	if filter.SortDirection == "" {
		filter.SortDirection = "DESC"
	}
	if filter.SortDirection != "ASC" && filter.SortDirection != "DESC" {
		return errors.New("export list sort_direction is invalid")
	}
	return nil
}

func exportTaskListWhere(tenantID ID, filter ExportTaskListFilter) (string, []any) {
	clauses := []string{"e.tenant_id = ?"}
	args := []any{tenantID}
	if filter.CreatedBy != "" {
		clauses, args = append(clauses, "e.created_by = ?"), append(args, filter.CreatedBy)
	}
	if filter.Search != "" {
		pattern := "%" + filter.Search + "%"
		clauses = append(clauses, `(e.id LIKE ? OR COALESCE(e.target_id, '') LIKE ? OR COALESCE(e.port_id, '') LIKE ? OR e.dataset_key LIKE ?)`)
		args = append(args, pattern, pattern, pattern, pattern)
	}
	if filter.Status != "" {
		clauses, args = append(clauses, exportTaskStatusExpression()+" = ?"), append(args, filter.Status)
	}
	if filter.ValueLayer != "" {
		clauses, args = append(clauses, "e.value_layer = ?"), append(args, filter.ValueLayer)
	}
	if filter.Format != "" {
		clauses, args = append(clauses, "e.format = ?"), append(args, filter.Format)
	}
	return strings.Join(clauses, " AND "), args
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

func (s *MySQLStore) DeleteExportTask(ctx context.Context, tenantID, taskID ID) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var contractVersion uint16
	var storedStatus ExportStatus
	var operationJobID ID
	if err := tx.QueryRowContext(ctx, `
		SELECT contract_version, status, COALESCE(operation_job_id, '')
		FROM export_tasks WHERE tenant_id = ? AND id = ? FOR UPDATE
	`, tenantID, taskID).Scan(&contractVersion, &storedStatus, &operationJobID); err != nil {
		return err
	}
	effectiveStatus := storedStatus
	if contractVersion == ExportExecutionContractVersion && operationJobID != "" {
		var jobStatus string
		if err := tx.QueryRowContext(ctx, `SELECT status FROM operation_jobs WHERE tenant_id = ? AND id = ?`, tenantID, operationJobID).Scan(&jobStatus); err != nil {
			return err
		}
		effectiveStatus = exportStatusFromOperationJob(jobStatus, storedStatus)
	}
	if effectiveStatus != ExportStatusComplete && effectiveStatus != ExportStatusFailed && effectiveStatus != ExportStatusCanceled {
		return ErrExportNotTerminal
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM export_tasks WHERE tenant_id = ? AND id = ?`, tenantID, taskID)
	if err := requireOneExportRow(result, err); err != nil {
		return err
	}
	return tx.Commit()
}

// QueueExpiredExportDeletes snapshots artifact metadata into the durable job
// payload before any row is removed. The anti-join makes drainPurge advance to
// the next batch and the job idempotency key also closes concurrent scans.
func (s *MySQLStore) QueueExpiredExportDeletes(ctx context.Context, now time.Time, limit int) (int64, error) {
	if limit <= 0 || limit > maintenancePurgeBatch {
		limit = maintenancePurgeBatch
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT e.id, e.tenant_id, COALESCE(e.file_ref, ''), COALESCE(e.checksum, ''),
		       COALESCE(e.row_count, 0), COALESCE(e.size_bytes, 0)
		FROM export_tasks e
		LEFT JOIN operation_jobs deletion
		  ON deletion.tenant_id = e.tenant_id
		 AND deletion.job_type = 'export_delete'
		 AND deletion.idempotency_key = CONCAT('export-delete:', e.id)
		WHERE e.expires_at IS NOT NULL AND e.expires_at <= ? AND deletion.id IS NULL
		ORDER BY e.expires_at, e.id
		LIMIT ?
	`, now.UTC(), limit)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	var tasks []ExportTask
	for rows.Next() {
		var task ExportTask
		if err := rows.Scan(&task.ID, &task.TenantID, &task.FileRef, &task.Checksum, &task.RowCount, &task.SizeBytes); err != nil {
			return 0, err
		}
		tasks = append(tasks, task)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	var enqueued int64
	for _, task := range tasks {
		payload, requestHash, err := EncodeExportDeletePayload(task)
		if err != nil {
			return enqueued, err
		}
		if _, err := s.EnqueueOperationJob(ctx, OperationJob{
			TenantID: task.TenantID, ScopeType: OperationJobScopeTenant, JobType: ExportDeleteJobType,
			IdempotencyKey: "export-delete:" + string(task.ID), RequestHash: requestHash,
			CheckpointJSON: payload,
		}); err != nil {
			return enqueued, err
		}
		enqueued++
	}
	return enqueued, nil
}

func normalizeExportTask(task ExportTask) ExportTask {
	if task.PeriodType == "" {
		task.PeriodType = PeriodCustom
	}
	if task.Step == 0 && task.DatasetKey != FlowTrafficDataset {
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
		       ` + exportTaskStatusExpression() + `,
		       COALESCE(e.file_ref, ''), COALESCE(e.checksum, ''), COALESCE(e.size_bytes, 0), e.expires_at,
		       CASE WHEN e.contract_version = 1 AND oj.last_error_detail IS NOT NULL
		         THEN oj.last_error_detail ELSE COALESCE(e.error_message, '') END,
		       e.row_version, e.created_at, e.updated_at
		FROM export_tasks e
		LEFT JOIN operation_jobs oj ON oj.id = e.operation_job_id
	`
}

func exportTaskStatusExpression() string {
	return `CASE WHEN e.contract_version = 1 THEN
		CASE oj.status
			WHEN 'queued' THEN 'pending'
			WHEN 'running' THEN 'running'
			WHEN 'cancel_requested' THEN 'running'
			WHEN 'succeeded' THEN 'complete'
			WHEN 'failed' THEN 'failed'
			WHEN 'canceled' THEN 'canceled'
			ELSE e.status
		END
	ELSE e.status END`
}

func exportStatusFromOperationJob(jobStatus string, fallback ExportStatus) ExportStatus {
	switch jobStatus {
	case OperationJobStatusQueued:
		return ExportStatusPending
	case OperationJobStatusRunning, OperationJobStatusCancelRequested:
		return ExportStatusRunning
	case OperationJobStatusSucceeded:
		return ExportStatusComplete
	case OperationJobStatusFailed:
		return ExportStatusFailed
	case OperationJobStatusCanceled:
		return ExportStatusCanceled
	default:
		return fallback
	}
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
var _ ExportPageRepository = (*MySQLStore)(nil)
var _ ExportDeletionRepository = (*MySQLStore)(nil)
var _ PendingExportRepository = (*MySQLStore)(nil)
var _ = sql.ErrNoRows
