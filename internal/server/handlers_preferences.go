package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/gin-gonic/gin"
)

// User UI preferences live in MySQL: one row per user holding an opaque JSON
// settings object under optimistic concurrency (row_version exposed as a quoted
// ETag; PUT honours If-Match so concurrent tabs cannot silently clobber). Reads
// never write — a user with no row gets an empty {} default at version 0.
// Faithful de-tenant port of internal/watchdog user_preferences.

var errUserPreferencesConflict = errors.New("user preferences changed since read")

func (s *Server) getUserPreferences(c *gin.Context) {
	settings, rowVersion, err := s.loadUserPreferences(c.Request.Context(), currentPrincipal(c).UserID)
	if err != nil {
		fail(c, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	c.Header("ETag", etag(rowVersion))
	c.JSON(http.StatusOK, gin.H{"settings": json.RawMessage(settings), "row_version": rowVersion})
}

func (s *Server) putUserPreferences(c *gin.Context) {
	body, err := io.ReadAll(io.LimitReader(c.Request.Body, 1<<20))
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "failed to read request body")
		return
	}
	// The stored value must be a JSON object; a scalar or array would break the
	// merge/read contract the frontend relies on.
	var probe map[string]any
	if err := json.Unmarshal(body, &probe); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "preferences body must be a JSON object")
		return
	}
	// Compact so stored JSON is canonical and small.
	var compact bytes.Buffer
	if err := json.Compact(&compact, body); err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "invalid JSON body")
		return
	}
	expected, ok := parsePreferencesIfMatch(c)
	if !ok {
		return
	}
	settings, rowVersion, err := s.upsertUserPreferences(c.Request.Context(), currentPrincipal(c).UserID, compact.Bytes(), expected)
	if err != nil {
		if errors.Is(err, errUserPreferencesConflict) {
			fail(c, http.StatusPreconditionFailed, "version_conflict", "preferences changed since they were read; re-read and retry")
			return
		}
		fail(c, http.StatusInternalServerError, "internal", err.Error())
		return
	}
	c.Header("ETag", etag(rowVersion))
	c.JSON(http.StatusOK, gin.H{"settings": json.RawMessage(settings), "row_version": rowVersion})
}

// loadUserPreferences returns the stored settings JSON and row_version, or an
// empty {} default at version 0 when the user has no row yet (never writes).
func (s *Server) loadUserPreferences(ctx context.Context, userID string) ([]byte, uint64, error) {
	var settings []byte
	var rowVersion uint64
	err := s.db.QueryRowContext(ctx, `SELECT settings_json, row_version FROM user_preferences WHERE user_id = ?`, userID).
		Scan(&settings, &rowVersion)
	if errors.Is(err, sql.ErrNoRows) {
		return []byte("{}"), 0, nil
	}
	if err != nil {
		return nil, 0, err
	}
	if len(settings) == 0 {
		settings = []byte("{}")
	}
	return settings, rowVersion, nil
}

// upsertUserPreferences creates the row (expected == 0) or updates it, returning
// errUserPreferencesConflict on a version mismatch.
func (s *Server) upsertUserPreferences(ctx context.Context, userID string, settings []byte, expected uint64) ([]byte, uint64, error) {
	if len(settings) == 0 {
		settings = []byte("{}")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, 0, err
	}
	defer tx.Rollback()

	var current uint64
	err = tx.QueryRowContext(ctx, `SELECT row_version FROM user_preferences WHERE user_id = ? FOR UPDATE`, userID).Scan(&current)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		if expected != 0 {
			return nil, 0, errUserPreferencesConflict
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO user_preferences (user_id, settings_json) VALUES (?, ?)`, userID, string(settings)); err != nil {
			return nil, 0, err
		}
	case err != nil:
		return nil, 0, err
	default:
		if expected != current {
			return nil, 0, errUserPreferencesConflict
		}
		if _, err := tx.ExecContext(ctx, `UPDATE user_preferences SET settings_json = ?, row_version = row_version + 1 WHERE user_id = ? AND row_version = ?`,
			string(settings), userID, expected); err != nil {
			return nil, 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, 0, err
	}
	return s.loadUserPreferences(ctx, userID)
}

// parsePreferencesIfMatch returns the expected row version: a missing If-Match
// maps to 0 (create-or-first-write); a present one must be a quoted uint.
func parsePreferencesIfMatch(c *gin.Context) (uint64, bool) {
	raw := c.GetHeader("If-Match")
	if raw == "" || raw == "*" {
		return 0, true
	}
	if len(raw) < 3 || raw[0] != '"' || raw[len(raw)-1] != '"' {
		fail(c, http.StatusBadRequest, "invalid_request", "If-Match must be a quoted row_version")
		return 0, false
	}
	version, err := strconv.ParseUint(raw[1:len(raw)-1], 10, 64)
	if err != nil {
		fail(c, http.StatusBadRequest, "invalid_request", "If-Match must be a quoted row_version")
		return 0, false
	}
	return version, true
}
