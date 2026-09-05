package watchdog

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

// PLAT P0: user preferences live in MySQL (was the PocketBase user_settings
// collection). A user has at most one row; reads return an empty default
// without writing, and writes create-or-update under optimistic concurrency.

var ErrUserPreferencesConflict = errors.New("user preferences changed since read")

type UserPreferences struct {
	UserID     ID              `json:"user_id"`
	TenantID   ID              `json:"tenant_id"`
	Settings   json.RawMessage `json:"settings"`
	RowVersion uint64          `json:"row_version"`
	UpdatedAt  time.Time       `json:"updated_at,omitzero"`
}

type UserPreferencesRepository interface {
	// GetUserPreferences returns the stored preferences, or a zero-version
	// empty default ({}) when the user has no row yet — reads never write.
	GetUserPreferences(ctx context.Context, tenantID, userID ID) (UserPreferences, error)
	// UpsertUserPreferences creates the row (when expectedRowVersion is 0) or
	// updates it, returning ErrUserPreferencesConflict on a version mismatch.
	UpsertUserPreferences(ctx context.Context, prefs UserPreferences, expectedRowVersion uint64) (UserPreferences, error)
}

func (s *MySQLStore) GetUserPreferences(ctx context.Context, tenantID, userID ID) (UserPreferences, error) {
	var prefs UserPreferences
	var settings []byte
	err := s.db.QueryRowContext(ctx, `
		SELECT user_id, tenant_id, settings_json, row_version, updated_at
		FROM user_preferences
		WHERE tenant_id = ? AND user_id = ?
	`, tenantID, userID).Scan(&prefs.UserID, &prefs.TenantID, &settings, &prefs.RowVersion, &prefs.UpdatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return UserPreferences{UserID: userID, TenantID: tenantID, Settings: json.RawMessage("{}"), RowVersion: 0}, nil
	}
	if err != nil {
		return UserPreferences{}, err
	}
	prefs.Settings = settings
	return prefs, nil
}

func (s *MySQLStore) UpsertUserPreferences(ctx context.Context, prefs UserPreferences, expectedRowVersion uint64) (UserPreferences, error) {
	settings := prefs.Settings
	if len(settings) == 0 {
		settings = json.RawMessage("{}")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return UserPreferences{}, err
	}
	defer tx.Rollback()

	var currentVersion uint64
	err = tx.QueryRowContext(ctx, `
		SELECT row_version FROM user_preferences WHERE tenant_id = ? AND user_id = ? FOR UPDATE
	`, prefs.TenantID, prefs.UserID).Scan(&currentVersion)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if expectedRowVersion != 0 {
			return UserPreferences{}, ErrUserPreferencesConflict
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO user_preferences (user_id, tenant_id, settings_json)
			VALUES (?, ?, ?)
		`, prefs.UserID, prefs.TenantID, string(settings)); err != nil {
			return UserPreferences{}, err
		}
	case err != nil:
		return UserPreferences{}, err
	default:
		if expectedRowVersion != currentVersion {
			return UserPreferences{}, ErrUserPreferencesConflict
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE user_preferences
			SET settings_json = ?, row_version = row_version + 1
			WHERE tenant_id = ? AND user_id = ? AND row_version = ?
		`, string(settings), prefs.TenantID, prefs.UserID, expectedRowVersion); err != nil {
			return UserPreferences{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return UserPreferences{}, err
	}
	return s.GetUserPreferences(ctx, prefs.TenantID, prefs.UserID)
}
