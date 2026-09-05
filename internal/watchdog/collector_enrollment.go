package watchdog

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// PLAT-03C2 one-time enrollment. The admin stages the collector's identity
// and receives a single-use secret; the collector exchanges it exactly once
// for its registry row plus initial token. The exchange runs in one
// transaction with the secret row locked, so replays and races collapse into
// a single winner. The admin who minted the secret pre-authorized the
// collector, so enrollment activates it directly.

var (
	ErrEnrollmentSecretInvalid = errors.New("enrollment secret is invalid, expired, or already used")
)

const (
	enrollmentSecretDefaultTTL = time.Hour
	enrollmentSecretMaxTTL     = 24 * time.Hour
)

type CollectorEnrollmentSecret struct {
	ID                ID        `json:"id"`
	TenantID          ID        `json:"tenant_id"`
	Secret            string    `json:"secret,omitempty"` // returned exactly once, never stored
	ModuleKey         string    `json:"module_key"`
	AgentType         string    `json:"agent_type"`
	Mode              string    `json:"mode"`
	CollectorName     string    `json:"collector_name"`
	ExpiresAt         time.Time `json:"expires_at"`
	UsedAt            time.Time `json:"used_at,omitzero"`
	UsedByCollectorID ID        `json:"used_by_collector_id,omitempty"`
	CreatedBy         ID        `json:"created_by"`
	CreatedAt         time.Time `json:"created_at"`
}

type CollectorEnrollmentResult struct {
	CollectorID ID     `json:"collector_id"`
	TenantID    ID     `json:"tenant_id"`
	ModuleKey   string `json:"module_key"`
	AgentType   string `json:"agent_type"`
	Token       string `json:"token"` // returned exactly once, never stored
}

type CollectorEnrollmentRepository interface {
	CreateEnrollmentSecret(ctx context.Context, secret CollectorEnrollmentSecret, secretHash string) (CollectorEnrollmentSecret, error)
	ListEnrollmentSecrets(ctx context.Context, tenantID ID) ([]CollectorEnrollmentSecret, error)
	DeleteEnrollmentSecret(ctx context.Context, tenantID, secretID ID) error
	// ConsumeEnrollmentSecret atomically verifies and burns the secret, creates
	// the collector row and stores the initial token hash. verify receives the
	// stored secret hash and must return true for the exchange to proceed.
	ConsumeEnrollmentSecret(ctx context.Context, secretID ID, verify func(secretHash string) bool, tokenHash string) (CollectorEnrollmentResult, error)
}

func (s *MySQLStore) CreateEnrollmentSecret(ctx context.Context, secret CollectorEnrollmentSecret, secretHash string) (CollectorEnrollmentSecret, error) {
	if secret.ID == "" {
		id, err := newIdentityID()
		if err != nil {
			return CollectorEnrollmentSecret{}, err
		}
		secret.ID = id
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO collector_enrollment_secrets (
			id, tenant_id, secret_hash, module_key, agent_type, mode,
			collector_name, expires_at, created_by
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, secret.ID, secret.TenantID, secretHash, secret.ModuleKey, secret.AgentType,
		secret.Mode, secret.CollectorName, secret.ExpiresAt, secret.CreatedBy)
	if err != nil {
		return CollectorEnrollmentSecret{}, err
	}
	return secret, nil
}

func (s *MySQLStore) ListEnrollmentSecrets(ctx context.Context, tenantID ID) ([]CollectorEnrollmentSecret, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, tenant_id, module_key, agent_type, mode, collector_name,
			expires_at, used_at, COALESCE(used_by_collector_id, ''), created_by, created_at
		FROM collector_enrollment_secrets
		WHERE tenant_id = ?
		ORDER BY created_at DESC
	`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var secrets []CollectorEnrollmentSecret
	for rows.Next() {
		var secret CollectorEnrollmentSecret
		var usedAt sql.NullTime
		if err := rows.Scan(&secret.ID, &secret.TenantID, &secret.ModuleKey, &secret.AgentType,
			&secret.Mode, &secret.CollectorName, &secret.ExpiresAt, &usedAt,
			&secret.UsedByCollectorID, &secret.CreatedBy, &secret.CreatedAt); err != nil {
			return nil, err
		}
		if usedAt.Valid {
			secret.UsedAt = usedAt.Time
		}
		secrets = append(secrets, secret)
	}
	return secrets, rows.Err()
}

func (s *MySQLStore) DeleteEnrollmentSecret(ctx context.Context, tenantID, secretID ID) error {
	result, err := s.db.ExecContext(ctx, `
		DELETE FROM collector_enrollment_secrets
		WHERE tenant_id = ? AND id = ? AND used_at IS NULL
	`, tenantID, secretID)
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

func (s *MySQLStore) ConsumeEnrollmentSecret(ctx context.Context, secretID ID, verify func(secretHash string) bool, tokenHash string) (CollectorEnrollmentResult, error) {
	if verify == nil || tokenHash == "" {
		return CollectorEnrollmentResult{}, ErrEnrollmentSecretInvalid
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return CollectorEnrollmentResult{}, err
	}
	defer tx.Rollback()

	var secret CollectorEnrollmentSecret
	var secretHash string
	err = tx.QueryRowContext(ctx, `
		SELECT id, tenant_id, secret_hash, module_key, agent_type, mode, collector_name, created_by
		FROM collector_enrollment_secrets
		WHERE id = ? AND used_at IS NULL AND expires_at > ?
		FOR UPDATE
	`, secretID, time.Now().UTC()).Scan(&secret.ID, &secret.TenantID, &secretHash,
		&secret.ModuleKey, &secret.AgentType, &secret.Mode, &secret.CollectorName, &secret.CreatedBy)
	if errors.Is(err, sql.ErrNoRows) {
		return CollectorEnrollmentResult{}, ErrEnrollmentSecretInvalid
	}
	if err != nil {
		return CollectorEnrollmentResult{}, err
	}
	// A wrong secret value must leave the row unused: verification failure
	// rolls back without touching used_at.
	if !verify(secretHash) {
		return CollectorEnrollmentResult{}, ErrEnrollmentSecretInvalid
	}

	collectorID, err := newIdentityID()
	if err != nil {
		return CollectorEnrollmentResult{}, err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO collector_agents (
			id, tenant_id, module_key, name, agent_type, mode,
			status, observed_health, auth_type, token_hash, created_by, updated_by
		) VALUES (?, ?, ?, ?, ?, ?, 'active', 'unknown', 'token', ?, ?, ?)
	`, collectorID, secret.TenantID, secret.ModuleKey, secret.CollectorName,
		secret.AgentType, secret.Mode, tokenHash, secret.CreatedBy, secret.CreatedBy); err != nil {
		return CollectorEnrollmentResult{}, err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE collector_enrollment_secrets
		SET used_at = ?, used_by_collector_id = ?, updated_at = CURRENT_TIMESTAMP(3)
		WHERE id = ?
	`, time.Now().UTC(), collectorID, secretID); err != nil {
		return CollectorEnrollmentResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return CollectorEnrollmentResult{}, err
	}
	return CollectorEnrollmentResult{
		CollectorID: collectorID, TenantID: secret.TenantID,
		ModuleKey: secret.ModuleKey, AgentType: secret.AgentType,
	}, nil
}
