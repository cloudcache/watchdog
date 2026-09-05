package watchdog

import (
	"context"
	"encoding/json"
	"time"
)

// PLAT-04 verifiable destruction. When an async delete job finishes it records
// a durable receipt in audit_logs so what was destroyed stays queryable after
// the rows are gone. The receipt id is derived from the job id, and the insert
// is idempotent, so a retried or taken-over job writes exactly one receipt.

type DestructionReceipt struct {
	TenantID     ID
	JobID        ID
	ResourceType string
	ResourceID   ID
	ActorID      ID
	Impact       map[string]int
	SeriesMatch  string
	DestroyedAt  time.Time
}

type DestructionReceiptRecorder interface {
	RecordDestructionReceipt(ctx context.Context, receipt DestructionReceipt) error
}

func (s *MySQLStore) RecordDestructionReceipt(ctx context.Context, receipt DestructionReceipt) error {
	detail := map[string]any{
		"job_id":       string(receipt.JobID),
		"destroyed_at": receipt.DestroyedAt.UTC().Format(time.RFC3339Nano),
	}
	if len(receipt.Impact) > 0 {
		detail["impact"] = receipt.Impact
	}
	if receipt.SeriesMatch != "" {
		detail["series_match"] = receipt.SeriesMatch
	}
	// Non-user actors (or an empty actor) never satisfy the audit actor FK; keep
	// the raw value in the detail and leave actor_id NULL.
	actorID := receipt.ActorID
	if actorID != "" {
		var exists int
		if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM users WHERE id = ?`, actorID).Scan(&exists); err != nil {
			return err
		}
		if exists == 0 {
			detail["actor"] = string(actorID)
			actorID = ""
		}
	}
	detailJSON, err := json.Marshal(detail)
	if err != nil {
		return err
	}
	// Idempotent by construction: the id is deterministic from the job, so a
	// retried job's second receipt collapses onto the first.
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO audit_logs (id, tenant_id, actor_id, action, resource_type, resource_id, detail_json, created_at)
		VALUES (?, ?, NULLIF(?, ''), ?, ?, NULLIF(?, ''), ?, ?)
		ON DUPLICATE KEY UPDATE id = id
	`, stableID("destroy", string(receipt.JobID)), receipt.TenantID, actorID,
		receipt.ResourceType+".destroyed", receipt.ResourceType, receipt.ResourceID,
		detailJSON, receipt.DestroyedAt.UTC())
	return err
}
