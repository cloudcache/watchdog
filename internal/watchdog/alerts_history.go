package watchdog

import (
	"context"
	"database/sql"
	"strings"
	"time"
)

// PLAT P0 alert subsystem: alert history lives in MySQL. The PocketBase alert
// engine fires create/resolve on its alerts collection; those events write here
// by resolving the PocketBase user id to the MySQL user. The user reads and
// deletes only their own history.

// alertHistoryKeep / alertHistoryTrimAt reproduce the PocketBase retention:
// keep the newest N per user, and only trim once a user exceeds the high-water
// mark (so the trim query does not run on every insert).
const (
	alertHistoryKeep   = 200
	alertHistoryTrimAt = 250
)

type AlertHistoryEntry struct {
	ID       ID         `json:"id"`
	AlertID  string     `json:"alert_id"`
	SystemID string     `json:"system"`
	Name     string     `json:"name"`
	Value    float64    `json:"value"`
	Created  time.Time  `json:"created"`
	Resolved *time.Time `json:"resolved"`
}

type AlertHistoryPageFilter struct {
	Limit  int
	Cursor string
}

type AlertHistoryRepository interface {
	ListAlertHistory(ctx context.Context, tenantID, userID ID, filter AlertHistoryPageFilter) ([]AlertHistoryEntry, string, error)
	DeleteAlertHistory(ctx context.Context, tenantID, userID, id ID) error
}

// CreateAlertHistoryForExternalSubject records a triggered alert for the user
// identified by a PocketBase id. An unknown subject is a no-op (the projection
// may lag), never an error, so alert processing is not blocked.
func (s *MySQLStore) CreateAlertHistoryForExternalSubject(ctx context.Context, provider, externalSubject, alertID, systemID, name string, value float64) error {
	provider = strings.ToLower(strings.TrimSpace(provider))
	externalSubject = strings.TrimSpace(externalSubject)
	if provider == "" || externalSubject == "" {
		return nil
	}
	var userID, tenantID ID
	err := s.db.QueryRowContext(ctx, `
		SELECT id, tenant_id FROM users WHERE auth_provider = ? AND external_subject_id = ?
	`, provider, externalSubject).Scan(&userID, &tenantID)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return err
	}
	id, err := newIdentityID()
	if err != nil {
		return err
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO alerts_history (id, tenant_id, user_id, alert_id, system_id, name, value)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, id, tenantID, userID, alertID, systemID, name, value); err != nil {
		return err
	}
	return s.trimAlertHistory(ctx, tenantID, userID)
}

// ResolveAlertHistory marks the open history row(s) for an alert resolved. It is
// a no-op when there is no open row (e.g. resolve without a prior trigger).
func (s *MySQLStore) ResolveAlertHistory(ctx context.Context, alertID string) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE alerts_history SET resolved_at = ? WHERE alert_id = ? AND resolved_at IS NULL
	`, time.Now().UTC(), alertID)
	return err
}

func (s *MySQLStore) trimAlertHistory(ctx context.Context, tenantID, userID ID) error {
	var count int
	if err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM alerts_history WHERE tenant_id = ? AND user_id = ?
	`, tenantID, userID).Scan(&count); err != nil {
		return err
	}
	if count <= alertHistoryTrimAt {
		return nil
	}
	// Delete everything older than the newest alertHistoryKeep rows. The extra
	// derived-table wrapper is required because MySQL cannot DELETE from a table
	// referenced directly in a subquery.
	_, err := s.db.ExecContext(ctx, `
		DELETE FROM alerts_history
		WHERE tenant_id = ? AND user_id = ? AND id NOT IN (
			SELECT id FROM (
				SELECT id FROM alerts_history
				WHERE tenant_id = ? AND user_id = ?
				ORDER BY created_at DESC, id DESC
				LIMIT ?
			) keep
		)
	`, tenantID, userID, tenantID, userID, alertHistoryKeep)
	return err
}

func (s *MySQLStore) ListAlertHistory(ctx context.Context, tenantID, userID ID, filter AlertHistoryPageFilter) ([]AlertHistoryEntry, string, error) {
	limit := filter.Limit
	if limit <= 0 || limit > 500 {
		limit = 200
	}
	query := `
		SELECT id, alert_id, system_id, name, value, created_at, resolved_at
		FROM alerts_history WHERE tenant_id = ? AND user_id = ?`
	args := []any{tenantID, userID}
	if filter.Cursor != "" {
		created, id, err := decodeAuditCursor(filter.Cursor)
		if err != nil {
			return nil, "", err
		}
		query += ` AND (created_at < ? OR (created_at = ? AND id < ?))`
		args = append(args, created.UTC(), created.UTC(), id)
	}
	query += ` ORDER BY created_at DESC, id DESC LIMIT ?`
	args = append(args, limit+1)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	var entries []AlertHistoryEntry
	for rows.Next() {
		var e AlertHistoryEntry
		var resolved sql.NullTime
		if err := rows.Scan(&e.ID, &e.AlertID, &e.SystemID, &e.Name, &e.Value, &e.Created, &resolved); err != nil {
			return nil, "", err
		}
		if resolved.Valid {
			t := resolved.Time
			e.Resolved = &t
		}
		entries = append(entries, e)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	nextCursor := ""
	if len(entries) > limit {
		entries = entries[:limit]
		last := entries[limit-1]
		nextCursor = encodeAuditCursor(last.Created, last.ID)
	}
	return entries, nextCursor, nil
}

func (s *MySQLStore) DeleteAlertHistory(ctx context.Context, tenantID, userID, id ID) error {
	result, err := s.db.ExecContext(ctx, `
		DELETE FROM alerts_history WHERE tenant_id = ? AND user_id = ? AND id = ?
	`, tenantID, userID, id)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		return sql.ErrNoRows
	}
	return nil
}
