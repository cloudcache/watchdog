package watchdog

import (
	"context"
	"strings"
)

const addressDimensionConsumerCTE = `
	WITH target AS (
		SELECT ? AS snapshot_id, ? AS version
	), ranked_attempts AS (
		SELECT acknowledgements.*,
		       ROW_NUMBER() OVER (
		         PARTITION BY acknowledgements.worker_id
		         ORDER BY acknowledgements.attempted_at DESC,
		                  acknowledgements.row_version DESC,
		                  acknowledgements.snapshot_id DESC
		       ) AS rank_number
		FROM dimension_snapshot_acks AS acknowledgements
		JOIN dimension_snapshots AS snapshots
		  ON snapshots.tenant_id = acknowledgements.tenant_id
		 AND snapshots.id = acknowledgements.snapshot_id
		WHERE acknowledgements.tenant_id = ?
		  AND snapshots.module_key = 'flow'
		  AND snapshots.dimension_key = 'address'
	), latest_attempts AS (
		SELECT * FROM ranked_attempts WHERE rank_number = 1
	), ranked_installed AS (
		SELECT acknowledgements.*,
		       snapshots.version AS installed_version,
		       ROW_NUMBER() OVER (
		         PARTITION BY acknowledgements.worker_id
		         ORDER BY acknowledgements.installed_at DESC,
		                  snapshots.version DESC,
		                  acknowledgements.snapshot_id DESC
		       ) AS rank_number
		FROM dimension_snapshot_acks AS acknowledgements
		JOIN dimension_snapshots AS snapshots
		  ON snapshots.tenant_id = acknowledgements.tenant_id
		 AND snapshots.id = acknowledgements.snapshot_id
		WHERE acknowledgements.tenant_id = ?
		  AND acknowledgements.state = 'installed'
		  AND snapshots.module_key = 'flow'
		  AND snapshots.dimension_key = 'address'
	), latest_installed AS (
		SELECT * FROM ranked_installed WHERE rank_number = 1
	), target_acknowledgements AS (
		SELECT acknowledgements.*
		FROM dimension_snapshot_acks AS acknowledgements
		JOIN target ON target.snapshot_id = acknowledgements.snapshot_id
		WHERE acknowledgements.tenant_id = ?
	), consumers AS (
		SELECT latest_attempts.worker_id,
		       COALESCE(target_acknowledgements.boot_id, latest_attempts.boot_id) AS boot_id,
		       COALESCE(target_acknowledgements.software_version, latest_attempts.software_version) AS software_version,
		       CASE target_acknowledgements.state
		         WHEN 'installed' THEN 'ready'
		         WHEN 'downloaded' THEN 'downloaded'
		         WHEN 'failed' THEN 'failed'
		         ELSE 'unreported'
		       END AS target_state,
		       target_acknowledgements.attempted_at AS target_attempted_at,
		       target_acknowledgements.installed_at AS target_installed_at,
		       COALESCE(target_acknowledgements.error_code, '') AS target_error_code,
		       COALESCE(target_acknowledgements.error_message, '') AS target_error_message,
		       COALESCE(latest_installed.snapshot_id, '') AS latest_installed_snapshot_id,
		       COALESCE(latest_installed.installed_version, 0) AS latest_installed_version,
		       latest_installed.installed_at AS latest_installed_at,
		       CASE
		         WHEN latest_installed.snapshot_id IS NULL THEN 'uninstalled'
		         WHEN latest_installed.snapshot_id = target.snapshot_id THEN 'current'
		         WHEN latest_installed.installed_version < target.version THEN 'behind'
		         WHEN latest_installed.installed_version > target.version THEN 'ahead'
		         ELSE 'uninstalled'
		       END AS drift
		FROM latest_attempts
		CROSS JOIN target
		LEFT JOIN target_acknowledgements
		  ON target_acknowledgements.worker_id = latest_attempts.worker_id
		LEFT JOIN latest_installed
		  ON latest_installed.worker_id = latest_attempts.worker_id
	)
`

const addressDimensionConsumerColumns = `
	worker_id, boot_id, software_version, target_state, target_attempted_at,
	target_installed_at, target_error_code, target_error_message,
	latest_installed_snapshot_id, latest_installed_version, latest_installed_at, drift`

func (p *MySQLAddressDimensionPublisher) GetAddressDimensionConsumerSummary(ctx context.Context, tenantID, snapshotID ID) (AddressDimensionConsumerSummary, error) {
	if p == nil || p.store == nil || tenantID == "" || snapshotID == "" {
		return AddressDimensionConsumerSummary{}, ErrAddressDimensionInvalid
	}
	snapshot, err := p.GetAddressDimensionSnapshot(ctx, tenantID, snapshotID)
	if err != nil {
		return AddressDimensionConsumerSummary{}, err
	}
	summary := AddressDimensionConsumerSummary{
		SnapshotID:   snapshot.ID,
		Version:      snapshot.Version,
		Scope:        AddressDimensionConsumerScopeObserved,
		Queryability: AddressDimensionQueryabilityUnknown,
	}
	err = p.store.db.QueryRowContext(ctx, addressDimensionConsumerCTE+`
		SELECT COUNT(*),
		       COALESCE(SUM(target_state = 'ready'), 0),
		       COALESCE(SUM(target_state = 'downloaded'), 0),
		       COALESCE(SUM(target_state = 'failed'), 0),
		       COALESCE(SUM(target_state = 'unreported'), 0),
		       COALESCE(SUM(drift = 'current'), 0),
		       COALESCE(SUM(drift = 'behind'), 0),
		       COALESCE(SUM(drift = 'ahead'), 0),
		       COALESCE(SUM(drift = 'uninstalled'), 0)
		FROM consumers
	`, snapshot.ID, snapshot.Version, tenantID, tenantID, tenantID).Scan(
		&summary.Observed, &summary.Ready, &summary.Downloaded, &summary.Failed,
		&summary.Unreported, &summary.Current, &summary.Behind, &summary.Ahead, &summary.Uninstalled,
	)
	if err != nil {
		return AddressDimensionConsumerSummary{}, err
	}
	summary.Queryability = addressDimensionObservedQueryability(summary.Observed, summary.Ready)
	return summary, nil
}

func (p *MySQLAddressDimensionPublisher) ListAddressDimensionConsumers(ctx context.Context, tenantID, snapshotID ID, filter AddressDimensionConsumerFilter) ([]AddressDimensionConsumerStatus, string, error) {
	if p == nil || p.store == nil || tenantID == "" || snapshotID == "" {
		return nil, "", ErrAddressDimensionInvalid
	}
	filter.Query = strings.TrimSpace(filter.Query)
	filter.State = strings.TrimSpace(filter.State)
	filter.Drift = strings.TrimSpace(filter.Drift)
	if !validAddressDimensionConsumerStateFilter(filter.State) || !validAddressDimensionDriftFilter(filter.Drift) {
		return nil, "", ErrAddressDimensionInvalid
	}
	if filter.Limit <= 0 {
		filter.Limit = 100
	}
	if filter.Limit > 500 {
		filter.Limit = 500
	}
	snapshot, err := p.GetAddressDimensionSnapshot(ctx, tenantID, snapshotID)
	if err != nil {
		return nil, "", err
	}
	query := addressDimensionConsumerCTE + `SELECT ` + addressDimensionConsumerColumns + ` FROM consumers WHERE 1 = 1`
	args := []any{snapshot.ID, snapshot.Version, tenantID, tenantID, tenantID}
	if filter.Cursor != "" {
		workerID, cursorID, err := decodeStringCursor(filter.Cursor)
		if err != nil || workerID == "" || string(cursorID) != workerID {
			return nil, "", ErrAddressDimensionInvalid
		}
		query += ` AND worker_id > ?`
		args = append(args, workerID)
	}
	if filter.Query != "" {
		like := "%" + escapeSQLLike(filter.Query) + "%"
		query += ` AND (worker_id LIKE ? OR boot_id LIKE ? OR software_version LIKE ?)`
		args = append(args, like, like, like)
	}
	if filter.State != "" {
		query += ` AND target_state = ?`
		args = append(args, filter.State)
	}
	if filter.Drift != "" {
		query += ` AND drift = ?`
		args = append(args, filter.Drift)
	}
	query += ` ORDER BY worker_id LIMIT ?`
	args = append(args, filter.Limit+1)
	rows, err := p.store.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	items := make([]AddressDimensionConsumerStatus, 0, filter.Limit)
	for rows.Next() {
		var item AddressDimensionConsumerStatus
		if err := rows.Scan(
			&item.WorkerID, &item.BootID, &item.SoftwareVersion, &item.TargetState,
			&item.TargetAttemptedAt, &item.TargetInstalledAt, &item.TargetErrorCode,
			&item.TargetErrorMessage, &item.LatestInstalledSnapshot,
			&item.LatestInstalledVersion, &item.LatestInstalledAt, &item.Drift,
		); err != nil {
			return nil, "", err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	next := ""
	if len(items) > filter.Limit {
		items = items[:filter.Limit]
		last := items[len(items)-1].WorkerID
		next = encodeStringCursor(last, ID(last))
	}
	return items, next, nil
}

func addressDimensionObservedQueryability(observed, ready uint64) string {
	switch {
	case observed == 0:
		return AddressDimensionQueryabilityUnknown
	case ready == observed:
		return AddressDimensionQueryabilityReady
	case ready > 0:
		return AddressDimensionQueryabilityPartial
	default:
		return AddressDimensionQueryabilityUnavailable
	}
}

func validAddressDimensionConsumerStateFilter(state string) bool {
	return state == "" || state == AddressDimensionConsumerReady || state == AddressDimensionConsumerDownloaded ||
		state == AddressDimensionConsumerFailed || state == AddressDimensionConsumerUnreported
}

func validAddressDimensionDriftFilter(drift string) bool {
	return drift == "" || drift == AddressDimensionDriftCurrent || drift == AddressDimensionDriftBehind ||
		drift == AddressDimensionDriftAhead || drift == AddressDimensionDriftUninstalled
}

var _ AddressDimensionConsumerStatusReader = (*MySQLAddressDimensionPublisher)(nil)
