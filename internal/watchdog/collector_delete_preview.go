package watchdog

import (
	"context"
	"database/sql"
	"errors"
)

// PLAT-04 collector delete impact preview. Unlike target/device/port, a
// collector's ownership evidence (service principals, ownership transfers) is
// FK-RESTRICT: it is permanent machine-credential history and blocks a hard
// delete. The preview therefore reports two categories the others do not need:
// "deleted" cascades (bindings, plan revisions) and "blocking" evidence that
// must be revoked/cleared before the collector can be removed.

// ErrCollectorDeleteBlocked is returned when a collector still has RESTRICT
// evidence (principals or ownership transfers) that prevents deletion.
var ErrCollectorDeleteBlocked = errors.New("collector has ownership evidence and cannot be deleted; revoke it first")

type CollectorDeletePreview struct {
	CollectorID ID                   `json:"collector_id"`
	Name        string               `json:"name"`
	Deletable   bool                 `json:"deletable"`
	Impacts     []TargetDeleteImpact `json:"impacts"`
}

type CollectorDeletePreviewRepository interface {
	PreviewCollectorDelete(ctx context.Context, tenantID, collectorID ID) (CollectorDeletePreview, error)
}

func (s *MySQLStore) PreviewCollectorDelete(ctx context.Context, tenantID, collectorID ID) (CollectorDeletePreview, error) {
	var name string
	err := s.db.QueryRowContext(ctx, `
		SELECT name FROM collector_agents WHERE tenant_id = ? AND id = ? AND deleted_at IS NULL
	`, tenantID, collectorID).Scan(&name)
	if errors.Is(err, sql.ErrNoRows) {
		return CollectorDeletePreview{}, sql.ErrNoRows
	}
	if err != nil {
		return CollectorDeletePreview{}, err
	}
	preview := CollectorDeletePreview{CollectorID: collectorID, Name: name, Deletable: true}

	for _, counted := range []struct {
		resourceType string
		behavior     string
		detail       string
		query        string
	}{
		{"collector_binding", "deleted", "", `SELECT COUNT(*) FROM collector_bindings WHERE tenant_id = ? AND collector_id = ?`},
		{"collector_plan_revision", "deleted", "", `SELECT COUNT(*) FROM collector_plan_revisions WHERE tenant_id = ? AND collector_id = ?`},
		{"collector_service_principal", "blocking", "revoke and clear these Kafka service principals first", `SELECT COUNT(*) FROM collector_service_principals WHERE tenant_id = ? AND collector_id = ?`},
		{"collector_ownership_transfer", "blocking", "resolve these ownership transfers first", `SELECT COUNT(*) FROM collector_ownership_transfers WHERE tenant_id = ? AND (old_collector_id = ? OR new_collector_id = ?)`},
	} {
		var count int
		args := []any{tenantID, collectorID}
		if counted.resourceType == "collector_ownership_transfer" {
			args = []any{tenantID, collectorID, collectorID}
		}
		if err := s.db.QueryRowContext(ctx, counted.query, args...).Scan(&count); err != nil {
			return CollectorDeletePreview{}, err
		}
		if count > 0 {
			preview.Impacts = append(preview.Impacts, TargetDeleteImpact{
				ResourceType: counted.resourceType, Behavior: counted.behavior, Count: count, Detail: counted.detail,
			})
			if counted.behavior == "blocking" {
				preview.Deletable = false
			}
		}
	}
	return preview, nil
}

// DeleteCollector removes a registry collector row (its bindings and plan
// revisions cascade). It refuses when RESTRICT ownership evidence remains,
// returning ErrCollectorDeleteBlocked instead of surfacing a raw FK error.
func (s *MySQLStore) DeleteCollector(ctx context.Context, tenantID, collectorID ID) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var blockers int
	if err := tx.QueryRowContext(ctx, `
		SELECT
			(SELECT COUNT(*) FROM collector_service_principals WHERE tenant_id = ? AND collector_id = ?) +
			(SELECT COUNT(*) FROM collector_ownership_transfers WHERE tenant_id = ? AND (old_collector_id = ? OR new_collector_id = ?))
	`, tenantID, collectorID, tenantID, collectorID, collectorID).Scan(&blockers); err != nil {
		return err
	}
	if blockers > 0 {
		return ErrCollectorDeleteBlocked
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM collector_agents WHERE tenant_id = ? AND id = ?`, tenantID, collectorID)
	if err != nil {
		return err
	}
	if count, err := result.RowsAffected(); err != nil {
		return err
	} else if count == 0 {
		return sql.ErrNoRows
	}
	return tx.Commit()
}
