package watchdog

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"time"
)

// PLAT-03C2 dual-window credential rotation. The collector registry is the
// only credential write authority (PLAT-03B1): a rotation stages a pending
// credential beside the active one, both authenticate until the window
// closes, then commit promotes the pending credential (retiring the old one)
// or abort discards it. Revoke suspends the collector's machine access
// entirely. Every state change requires the caller's expected row_version so
// concurrent management writes conflict instead of interleaving.

var (
	ErrCollectorCredentialConflict = errors.New("collector credential state changed; re-read and retry")
	ErrCollectorNoPendingRotation  = errors.New("collector has no unexpired pending credential")
)

const (
	collectorRotationDefaultTTL = 24 * time.Hour
	collectorRotationMaxTTL     = 7 * 24 * time.Hour
)

type CollectorCredentialRotation struct {
	CollectorID ID        `json:"collector_id"`
	AuthType    string    `json:"auth_type"`
	Token       string    `json:"token,omitempty"` // returned exactly once, never stored
	ExpiresAt   time.Time `json:"expires_at"`
	RowVersion  uint64    `json:"row_version"`
}

type CollectorCredentialState struct {
	CollectorID ID
	TenantID    ID
	AuthType    string
	Status      string
	HasPending  bool
	ExpiresAt   time.Time
	RowVersion  uint64
}

type CollectorCredentialRepository interface {
	GetCollectorCredentialState(ctx context.Context, tenantID, collectorID ID) (CollectorCredentialState, error)
	StageCollectorCredential(ctx context.Context, tenantID, collectorID ID, expectedRowVersion uint64, pendingTokenHash, pendingFingerprint string, expiresAt time.Time, actor ID) error
	CommitCollectorCredential(ctx context.Context, tenantID, collectorID ID, expectedRowVersion uint64, actor ID) error
	AbortCollectorCredential(ctx context.Context, tenantID, collectorID ID, expectedRowVersion uint64, actor ID) error
	RevokeCollectorCredential(ctx context.Context, tenantID, collectorID ID, expectedRowVersion uint64, actor ID) error
}

// NewCollectorToken returns a fresh random bearer token. Only its bcrypt hash
// is stored; the cleartext is shown to the caller once.
func NewCollectorToken() (string, error) {
	buf := make([]byte, 32)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return "wdc_" + hex.EncodeToString(buf), nil
}

func (s *MySQLStore) GetCollectorCredentialState(ctx context.Context, tenantID, collectorID ID) (CollectorCredentialState, error) {
	var state CollectorCredentialState
	var expires sql.NullTime
	var pendingToken, pendingFingerprint sql.NullString
	err := s.db.QueryRowContext(ctx, `
		SELECT id, tenant_id, auth_type, status, pending_token_hash,
			pending_certificate_fingerprint, pending_credential_expires_at, row_version
		FROM collector_agents
		WHERE tenant_id = ? AND id = ? AND deleted_at IS NULL
	`, tenantID, collectorID).Scan(&state.CollectorID, &state.TenantID, &state.AuthType, &state.Status,
		&pendingToken, &pendingFingerprint, &expires, &state.RowVersion)
	if err != nil {
		return CollectorCredentialState{}, err
	}
	state.HasPending = pendingToken.Valid || pendingFingerprint.Valid
	if expires.Valid {
		state.ExpiresAt = expires.Time
	}
	return state, nil
}

func (s *MySQLStore) StageCollectorCredential(ctx context.Context, tenantID, collectorID ID, expectedRowVersion uint64, pendingTokenHash, pendingFingerprint string, expiresAt time.Time, actor ID) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE collector_agents
		SET pending_token_hash = NULLIF(?, ''),
			pending_certificate_fingerprint = NULLIF(?, ''),
			pending_credential_expires_at = ?,
			updated_by = ?, row_version = row_version + 1, updated_at = CURRENT_TIMESTAMP(3)
		WHERE tenant_id = ? AND id = ? AND row_version = ?
			AND status IN ('pending', 'active') AND deleted_at IS NULL
	`, pendingTokenHash, pendingFingerprint, expiresAt, actor, tenantID, collectorID, expectedRowVersion)
	return collectorCredentialWriteOutcome(result, err)
}

func (s *MySQLStore) CommitCollectorCredential(ctx context.Context, tenantID, collectorID ID, expectedRowVersion uint64, actor ID) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE collector_agents
		SET token_hash = COALESCE(pending_token_hash, token_hash),
			certificate_fingerprint = COALESCE(pending_certificate_fingerprint, certificate_fingerprint),
			pending_token_hash = NULL,
			pending_certificate_fingerprint = NULL,
			pending_credential_expires_at = NULL,
			updated_by = ?, row_version = row_version + 1, updated_at = CURRENT_TIMESTAMP(3)
		WHERE tenant_id = ? AND id = ? AND row_version = ?
			AND pending_credential_expires_at IS NOT NULL
			AND pending_credential_expires_at > ?
			AND deleted_at IS NULL
	`, actor, tenantID, collectorID, expectedRowVersion, time.Now().UTC())
	if err := collectorCredentialWriteOutcome(result, err); err != nil {
		if errors.Is(err, ErrCollectorCredentialConflict) {
			if state, stateErr := s.GetCollectorCredentialState(ctx, tenantID, collectorID); stateErr == nil &&
				state.RowVersion == expectedRowVersion &&
				(!state.HasPending || !state.ExpiresAt.After(time.Now().UTC())) {
				return ErrCollectorNoPendingRotation
			}
		}
		return err
	}
	return nil
}

func (s *MySQLStore) AbortCollectorCredential(ctx context.Context, tenantID, collectorID ID, expectedRowVersion uint64, actor ID) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE collector_agents
		SET pending_token_hash = NULL,
			pending_certificate_fingerprint = NULL,
			pending_credential_expires_at = NULL,
			updated_by = ?, row_version = row_version + 1, updated_at = CURRENT_TIMESTAMP(3)
		WHERE tenant_id = ? AND id = ? AND row_version = ? AND deleted_at IS NULL
	`, actor, tenantID, collectorID, expectedRowVersion)
	return collectorCredentialWriteOutcome(result, err)
}

func (s *MySQLStore) RevokeCollectorCredential(ctx context.Context, tenantID, collectorID ID, expectedRowVersion uint64, actor ID) error {
	result, err := s.db.ExecContext(ctx, `
		UPDATE collector_agents
		SET status = 'revoked',
			pending_token_hash = NULL,
			pending_certificate_fingerprint = NULL,
			pending_credential_expires_at = NULL,
			updated_by = ?, row_version = row_version + 1, updated_at = CURRENT_TIMESTAMP(3)
		WHERE tenant_id = ? AND id = ? AND row_version = ? AND deleted_at IS NULL
	`, actor, tenantID, collectorID, expectedRowVersion)
	return collectorCredentialWriteOutcome(result, err)
}

func collectorCredentialWriteOutcome(result sql.Result, err error) error {
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		return ErrCollectorCredentialConflict
	}
	return nil
}
