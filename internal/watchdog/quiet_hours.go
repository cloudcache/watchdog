package watchdog

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"time"
)

// PLAT P0 alert subsystem: quiet-hour windows live in MySQL. Users CRUD their
// own windows; the alert silencing path reads a user's windows by their
// PocketBase id (the identity projection's external subject).

type QuietHourWindow struct {
	ID        ID        `json:"id"`
	SystemID  string    `json:"system_id"`
	Type      string    `json:"type"`
	Start     time.Time `json:"start"`
	End       time.Time `json:"end"`
	CreatedAt time.Time `json:"created_at,omitzero"`
}

type QuietHoursRepository interface {
	ListQuietHours(ctx context.Context, tenantID, userID ID) ([]QuietHourWindow, error)
	CreateQuietHour(ctx context.Context, tenantID, userID ID, window QuietHourWindow) (QuietHourWindow, error)
	UpdateQuietHour(ctx context.Context, tenantID, userID ID, window QuietHourWindow) (QuietHourWindow, error)
	DeleteQuietHour(ctx context.Context, tenantID, userID, windowID ID) error
	// QuietHoursForExternalSubject resolves a PocketBase user id to the MySQL
	// user and returns the windows relevant to a system (global + that system).
	QuietHoursForExternalSubject(ctx context.Context, provider, externalSubject, systemID string) ([]QuietHourWindow, error)
}

func (s *MySQLStore) ListQuietHours(ctx context.Context, tenantID, userID ID) ([]QuietHourWindow, error) {
	return scanQuietHours(s.db.QueryContext(ctx, `
		SELECT id, system_id, window_type, start_at, end_at, created_at
		FROM quiet_hours WHERE tenant_id = ? AND user_id = ?
		ORDER BY created_at DESC, id DESC
	`, tenantID, userID))
}

func (s *MySQLStore) QuietHoursForExternalSubject(ctx context.Context, provider, externalSubject, systemID string) ([]QuietHourWindow, error) {
	provider = strings.ToLower(strings.TrimSpace(provider))
	externalSubject = strings.TrimSpace(externalSubject)
	if provider == "" || externalSubject == "" {
		return nil, nil
	}
	return scanQuietHours(s.db.QueryContext(ctx, `
		SELECT q.id, q.system_id, q.window_type, q.start_at, q.end_at, q.created_at
		FROM quiet_hours q
		INNER JOIN users u ON u.id = q.user_id
		WHERE u.auth_provider = ? AND u.external_subject_id = ?
			AND (q.system_id = '' OR q.system_id = ?)
	`, provider, externalSubject, systemID))
}

func (s *MySQLStore) CreateQuietHour(ctx context.Context, tenantID, userID ID, window QuietHourWindow) (QuietHourWindow, error) {
	id, err := newIdentityID()
	if err != nil {
		return QuietHourWindow{}, err
	}
	if _, err := s.db.ExecContext(ctx, `
		INSERT INTO quiet_hours (id, tenant_id, user_id, system_id, window_type, start_at, end_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, id, tenantID, userID, window.SystemID, window.Type, window.Start.UTC(), window.End.UTC()); err != nil {
		return QuietHourWindow{}, err
	}
	return s.getQuietHour(ctx, tenantID, userID, id)
}

func (s *MySQLStore) UpdateQuietHour(ctx context.Context, tenantID, userID ID, window QuietHourWindow) (QuietHourWindow, error) {
	result, err := s.db.ExecContext(ctx, `
		UPDATE quiet_hours SET system_id = ?, window_type = ?, start_at = ?, end_at = ?
		WHERE tenant_id = ? AND user_id = ? AND id = ?
	`, window.SystemID, window.Type, window.Start.UTC(), window.End.UTC(), tenantID, userID, window.ID)
	if err != nil {
		return QuietHourWindow{}, err
	}
	if count, err := result.RowsAffected(); err != nil {
		return QuietHourWindow{}, err
	} else if count == 0 {
		if _, getErr := s.getQuietHour(ctx, tenantID, userID, window.ID); getErr != nil {
			return QuietHourWindow{}, sql.ErrNoRows
		}
	}
	return s.getQuietHour(ctx, tenantID, userID, window.ID)
}

func (s *MySQLStore) DeleteQuietHour(ctx context.Context, tenantID, userID, windowID ID) error {
	result, err := s.db.ExecContext(ctx, `
		DELETE FROM quiet_hours WHERE tenant_id = ? AND user_id = ? AND id = ?
	`, tenantID, userID, windowID)
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

func (s *MySQLStore) getQuietHour(ctx context.Context, tenantID, userID, windowID ID) (QuietHourWindow, error) {
	windows, err := scanQuietHours(s.db.QueryContext(ctx, `
		SELECT id, system_id, window_type, start_at, end_at, created_at
		FROM quiet_hours WHERE tenant_id = ? AND user_id = ? AND id = ?
	`, tenantID, userID, windowID))
	if err != nil {
		return QuietHourWindow{}, err
	}
	if len(windows) == 0 {
		return QuietHourWindow{}, sql.ErrNoRows
	}
	return windows[0], nil
}

func scanQuietHours(rows *sql.Rows, scanErr error) ([]QuietHourWindow, error) {
	if scanErr != nil {
		return nil, scanErr
	}
	defer rows.Close()
	var windows []QuietHourWindow
	for rows.Next() {
		var window QuietHourWindow
		if err := rows.Scan(&window.ID, &window.SystemID, &window.Type, &window.Start, &window.End, &window.CreatedAt); err != nil {
			return nil, err
		}
		windows = append(windows, window)
	}
	return windows, rows.Err()
}

// ValidateQuietHourWindow checks the window type and that end is after start
// (for one-time windows; daily windows compare clock times so equal is fine).
func ValidateQuietHourWindow(window QuietHourWindow) error {
	if window.Type != "daily" && window.Type != "one-time" {
		return errors.New("quiet hour type must be 'daily' or 'one-time'")
	}
	if window.Start.IsZero() || window.End.IsZero() {
		return errors.New("quiet hour start and end are required")
	}
	if window.Type == "one-time" && !window.End.After(window.Start) {
		return errors.New("one-time quiet hour end must be after start")
	}
	return nil
}
