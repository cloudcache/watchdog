package watchdog

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

func (s *MySQLStore) CreateExportTask(ctx context.Context, task ExportTask) (ExportTask, error) {
	task = normalizeExportTask(task)
	if task.ID == "" {
		return ExportTask{}, errors.New("export task id is required")
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO export_tasks (
			id, tenant_id, created_by, target_id, port_id, period_type,
			range_start, range_end, step_seconds, aggregation, value_mode, format, status
		) VALUES (?, ?, ?, NULLIF(?, ''), NULLIF(?, ''), ?, ?, ?, ?, ?, ?, ?, ?)
	`, task.ID, task.TenantID, task.CreatedBy, task.TargetID, task.PortID, task.PeriodType, task.RangeStart, task.RangeEnd, uint32(task.Step.Seconds()), task.Aggregation, task.ValueMode, task.Format, task.Status)
	if err != nil {
		return ExportTask{}, err
	}
	return s.GetExportTask(ctx, task.TenantID, task.ID)
}

func (s *MySQLStore) GetExportTask(ctx context.Context, tenantID, taskID ID) (ExportTask, error) {
	row := s.db.QueryRowContext(ctx, exportTaskSelect()+`
		WHERE tenant_id = ? AND id = ?
	`, tenantID, taskID)
	return scanExportTask(row)
}

func (s *MySQLStore) ListExportTasks(ctx context.Context, tenantID ID, createdBy ID) ([]ExportTask, error) {
	query := exportTaskSelect() + ` WHERE tenant_id = ?`
	args := []any{tenantID}
	if createdBy != "" {
		query += ` AND created_by = ?`
		args = append(args, createdBy)
	}
	query += ` ORDER BY created_at DESC`
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
		WHERE status = ?
		ORDER BY created_at
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
	_, err := s.db.ExecContext(ctx, `
		UPDATE export_tasks
		SET status = ?, error_message = NULL, updated_at = CURRENT_TIMESTAMP(3)
		WHERE tenant_id = ? AND id = ?
	`, ExportStatusRunning, tenantID, taskID)
	return err
}

func (s *MySQLStore) RetryExportTask(ctx context.Context, tenantID, taskID ID) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE export_tasks
		SET status = ?, file_ref = NULL, error_message = NULL, updated_at = CURRENT_TIMESTAMP(3)
		WHERE tenant_id = ? AND id = ? AND status = ?
	`, ExportStatusPending, tenantID, taskID, ExportStatusFailed)
	return err
}

func (s *MySQLStore) MarkExportComplete(ctx context.Context, tenantID, taskID ID, fileRef string) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE export_tasks
		SET status = ?, file_ref = ?, error_message = NULL, updated_at = CURRENT_TIMESTAMP(3)
		WHERE tenant_id = ? AND id = ?
	`, ExportStatusComplete, fileRef, tenantID, taskID)
	return err
}

func (s *MySQLStore) MarkExportFailed(ctx context.Context, tenantID, taskID ID, message string) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE export_tasks
		SET status = ?, error_message = ?, updated_at = CURRENT_TIMESTAMP(3)
		WHERE tenant_id = ? AND id = ?
	`, ExportStatusFailed, message, tenantID, taskID)
	return err
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
	return task
}

func exportTaskSelect() string {
	return `
		SELECT id, tenant_id, created_by, COALESCE(target_id, ''), COALESCE(port_id, ''),
		       period_type, range_start, range_end, step_seconds, aggregation, value_mode,
		       format, status, COALESCE(file_ref, ''), COALESCE(error_message, ''),
		       created_at, updated_at
		FROM export_tasks
	`
}

func scanExportTask(row rowScanner) (ExportTask, error) {
	var task ExportTask
	var stepSeconds uint32
	err := row.Scan(
		&task.ID,
		&task.TenantID,
		&task.CreatedBy,
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
		&task.ErrorMessage,
		&task.CreatedAt,
		&task.UpdatedAt,
	)
	if err != nil {
		return task, err
	}
	task.Step = time.Duration(stepSeconds) * time.Second
	return task, nil
}

var _ ExportRepository = (*MySQLStore)(nil)
var _ PendingExportRepository = (*MySQLStore)(nil)
var _ = sql.ErrNoRows
