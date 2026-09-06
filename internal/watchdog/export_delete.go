package watchdog

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

const (
	ExportDeleteJobType        = "export_delete"
	ExportDeletePayloadVersion = 1
)

type exportDeleteJobPayload struct {
	ExportID ID     `json:"export_id"`
	FileRef  string `json:"file_ref,omitempty"`
	Checksum string `json:"checksum,omitempty"`
	RowCount uint64 `json:"row_count,omitempty"`
	Size     int64  `json:"size_bytes,omitempty"`
}

func EncodeExportDeletePayload(task ExportTask) (json.RawMessage, string, error) {
	payload, err := EncodeJobPayload(ExportDeletePayloadVersion, exportDeleteJobPayload{
		ExportID: task.ID, FileRef: task.FileRef, Checksum: task.Checksum,
		RowCount: task.RowCount, Size: task.SizeBytes,
	})
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(payload)
	return payload, hex.EncodeToString(sum[:]), nil
}

// NewExportDeleteJobHandler removes the artifact idempotently before deleting
// the management row. A crash between those steps is safe: retrying the file
// delete is a no-op, then the row and receipt converge.
func NewExportDeleteJobHandler(repo ExportDeletionRepository, files ExportFileDeleter, receipts DestructionReceiptRecorder) OperationJobHandler {
	return func(ctx context.Context, job OperationJob) (string, error) {
		var payload exportDeleteJobPayload
		if err := DecodeJobPayload(job.CheckpointJSON, ExportDeletePayloadVersion, &payload); err != nil {
			return "", err
		}
		if payload.ExportID == "" {
			return "", TerminalJobError(errors.New("export delete job payload has no export_id"))
		}
		if repo == nil || files == nil {
			return "", TerminalJobError(errors.New("export delete dependencies are not configured"))
		}
		if payload.FileRef != "" {
			if err := files.DeleteExport(ctx, payload.FileRef); err != nil {
				return "", err
			}
		}
		if err := repo.DeleteExportTask(ctx, job.TenantID, payload.ExportID); err != nil && !errors.Is(err, sql.ErrNoRows) {
			if errors.Is(err, ErrExportNotTerminal) {
				return "", TerminalJobError(err)
			}
			return "", err
		}
		if receipts != nil {
			impact := map[string]int{"artifacts": 1}
			maxInt := uint64(^uint(0) >> 1)
			if payload.RowCount <= maxInt {
				impact["rows"] = int(payload.RowCount)
			}
			if payload.Size >= 0 && uint64(payload.Size) <= maxInt {
				impact["bytes"] = int(payload.Size)
			}
			if err := receipts.RecordDestructionReceipt(ctx, DestructionReceipt{
				TenantID: job.TenantID, JobID: job.ID, ResourceType: string(ResourceExportTask),
				ResourceID: payload.ExportID, ActorID: job.CreatedBy,
				Impact: impact, DestroyedAt: time.Now().UTC(),
			}); err != nil {
				return "", err
			}
		}
		return fmt.Sprintf("deleted:%s", payload.ExportID), nil
	}
}
