package address

import (
	"context"
	"strings"
	"time"
)

const (
	AddressDimensionConsumerReady      = "ready"
	AddressDimensionConsumerDownloaded = "downloaded"
	AddressDimensionConsumerFailed     = "failed"
	AddressDimensionConsumerUnreported = "unreported"

	AddressDimensionDriftCurrent     = "current"
	AddressDimensionDriftBehind      = "behind"
	AddressDimensionDriftAhead       = "ahead"
	AddressDimensionDriftUninstalled = "uninstalled"

	AddressDimensionQueryabilityUnknown     = "unknown"
	AddressDimensionQueryabilityReady       = "ready_observed"
	AddressDimensionQueryabilityPartial     = "partial_observed"
	AddressDimensionQueryabilityUnavailable = "unavailable_observed"
	AddressDimensionConsumerScopeObserved   = "observed_only"
)

// AddressDimensionConsumerStatus separates target-version readiness from version
// drift: a worker may hold the target index while installing a newer version.
type AddressDimensionConsumerStatus struct {
	WorkerID                string     `json:"worker_id"`
	BootID                  string     `json:"boot_id"`
	SoftwareVersion         string     `json:"software_version"`
	TargetState             string     `json:"target_state"`
	TargetAttemptedAt       *time.Time `json:"target_attempted_at,omitempty"`
	TargetInstalledAt       *time.Time `json:"target_installed_at,omitempty"`
	TargetErrorCode         string     `json:"target_error_code,omitempty"`
	TargetErrorMessage      string     `json:"target_error_message,omitempty"`
	LatestInstalledSnapshot ID         `json:"latest_installed_snapshot_id,omitempty"`
	LatestInstalledVersion  uint64     `json:"latest_installed_version,omitempty"`
	LatestInstalledAt       *time.Time `json:"latest_installed_at,omitempty"`
	Drift                   string     `json:"drift"`
}

type AddressDimensionConsumerSummary struct {
	SnapshotID   ID     `json:"snapshot_id"`
	Version      uint64 `json:"version"`
	Scope        string `json:"scope"`
	Queryability string `json:"queryability"`
	Observed     uint64 `json:"observed"`
	Ready        uint64 `json:"ready"`
	Downloaded   uint64 `json:"downloaded"`
	Failed       uint64 `json:"failed"`
	Unreported   uint64 `json:"unreported"`
	Current      uint64 `json:"current"`
	Behind       uint64 `json:"behind"`
	Ahead        uint64 `json:"ahead"`
	Uninstalled  uint64 `json:"uninstalled"`
}

type AddressDimensionConsumerFilter struct {
	Query  string
	State  string
	Drift  string
	Limit  int
	Cursor string
}

// dimensionPublicationConsumerCTE ranks each worker's latest attempt and latest
// installed version and classifies drift against the target snapshot. De-tenanted:
// the snapshot joins are on id alone and scoped by module_key/dimension_key; it
// binds (snapshot_id, version, module, dimension, module, dimension).
const dimensionPublicationConsumerCTE = `
	WITH target AS (
		SELECT ? AS snapshot_id, ? AS version
	), ranked_attempts AS (
		SELECT acknowledgements.*,
		       ROW_NUMBER() OVER (
		         PARTITION BY acknowledgements.worker_id
		         ORDER BY acknowledgements.attempted_at DESC, acknowledgements.row_version DESC, acknowledgements.snapshot_id DESC
		       ) AS rank_number
		FROM dimension_snapshot_acks AS acknowledgements
		JOIN dimension_snapshots AS snapshots ON snapshots.id = acknowledgements.snapshot_id
		WHERE snapshots.module_key = ? AND snapshots.dimension_key = ?
	), latest_attempts AS (
		SELECT * FROM ranked_attempts WHERE rank_number = 1
	), ranked_installed AS (
		SELECT acknowledgements.*, snapshots.version AS installed_version,
		       ROW_NUMBER() OVER (
		         PARTITION BY acknowledgements.worker_id
		         ORDER BY acknowledgements.installed_at DESC, snapshots.version DESC, acknowledgements.snapshot_id DESC
		       ) AS rank_number
		FROM dimension_snapshot_acks AS acknowledgements
		JOIN dimension_snapshots AS snapshots ON snapshots.id = acknowledgements.snapshot_id
		WHERE acknowledgements.state = 'installed' AND snapshots.module_key = ? AND snapshots.dimension_key = ?
	), latest_installed AS (
		SELECT * FROM ranked_installed WHERE rank_number = 1
	), target_acknowledgements AS (
		SELECT acknowledgements.*
		FROM dimension_snapshot_acks AS acknowledgements
		JOIN target ON target.snapshot_id = acknowledgements.snapshot_id
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
		LEFT JOIN target_acknowledgements ON target_acknowledgements.worker_id = latest_attempts.worker_id
		LEFT JOIN latest_installed ON latest_installed.worker_id = latest_attempts.worker_id
	)
`

const addressDimensionConsumerColumns = `
	worker_id, boot_id, software_version, target_state, target_attempted_at,
	target_installed_at, target_error_code, target_error_message,
	latest_installed_snapshot_id, latest_installed_version, latest_installed_at, drift`

func (p *Publisher) GetAddressDimensionConsumerSummary(ctx context.Context, snapshotID ID) (AddressDimensionConsumerSummary, error) {
	if p == nil || p.store == nil || snapshotID == "" {
		return AddressDimensionConsumerSummary{}, ErrAddressDimensionInvalid
	}
	snapshot, err := p.GetDimensionPublicationSnapshot(ctx, snapshotID)
	if err != nil {
		return AddressDimensionConsumerSummary{}, err
	}
	summary := AddressDimensionConsumerSummary{
		SnapshotID: snapshot.ID, Version: snapshot.Version,
		Scope: AddressDimensionConsumerScopeObserved, Queryability: AddressDimensionQueryabilityUnknown,
	}
	err = p.store.db.QueryRowContext(ctx, dimensionPublicationConsumerCTE+`
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
	`, snapshot.ID, snapshot.Version, p.scope.ModuleKey, p.scope.DimensionKey, p.scope.ModuleKey, p.scope.DimensionKey).Scan(
		&summary.Observed, &summary.Ready, &summary.Downloaded, &summary.Failed,
		&summary.Unreported, &summary.Current, &summary.Behind, &summary.Ahead, &summary.Uninstalled,
	)
	if err != nil {
		return AddressDimensionConsumerSummary{}, err
	}
	summary.Queryability = addressDimensionObservedQueryability(summary.Observed, summary.Ready)
	return summary, nil
}

func (p *Publisher) ListAddressDimensionConsumers(ctx context.Context, snapshotID ID, filter AddressDimensionConsumerFilter) ([]AddressDimensionConsumerStatus, string, error) {
	if p == nil || p.store == nil || snapshotID == "" {
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
	snapshot, err := p.GetDimensionPublicationSnapshot(ctx, snapshotID)
	if err != nil {
		return nil, "", err
	}
	query := dimensionPublicationConsumerCTE + `SELECT ` + addressDimensionConsumerColumns + ` FROM consumers WHERE 1 = 1`
	args := []any{snapshot.ID, snapshot.Version, p.scope.ModuleKey, p.scope.DimensionKey, p.scope.ModuleKey, p.scope.DimensionKey}
	if filter.Cursor != "" {
		query += ` AND worker_id > ?`
		args = append(args, filter.Cursor)
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
		next = items[len(items)-1].WorkerID
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
