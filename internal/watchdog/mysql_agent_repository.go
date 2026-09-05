package watchdog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

const collectorCompatibilityActor ID = "system:compatibility"

func (s *MySQLStore) GetAgent(ctx context.Context, agentID ID) (SNMPAgentConfig, error) {
	var agent SNMPAgentConfig
	var lastSeen, lastRun, lastSuccess sql.NullTime
	var lastError sql.NullString
	err := s.db.QueryRowContext(ctx, `
		SELECT id, tenant_id, target_id, agent_type, mode, COALESCE(endpoint, ''), token_hash, status,
			last_seen_at, last_run_at, last_success_at, last_error, run_count, failure_count, created_at, updated_at
		FROM target_agents
		WHERE id = ?
	`, agentID).Scan(
		&agent.ID, &agent.TenantID, &agent.TargetID, &agent.AgentType, &agent.Mode, &agent.Endpoint, &agent.TokenHash, &agent.Status,
		&lastSeen, &lastRun, &lastSuccess, &lastError, &agent.RunCount, &agent.FailureCount, &agent.CreatedAt, &agent.UpdatedAt,
	)
	if lastSeen.Valid {
		agent.LastSeen = lastSeen.Time
	}
	if lastRun.Valid {
		agent.LastRun = lastRun.Time
	}
	if lastSuccess.Valid {
		agent.LastSuccess = lastSuccess.Time
	}
	if lastError.Valid {
		agent.LastError = lastError.String
	}
	return NormalizeAgentConfig(agent), err
}

func (s *MySQLStore) ListAgents(ctx context.Context, tenantID ID) ([]SNMPAgentConfig, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, tenant_id, target_id, agent_type, mode, COALESCE(endpoint, ''), token_hash, status,
			last_seen_at, last_run_at, last_success_at, last_error, run_count, failure_count, created_at, updated_at
		FROM target_agents
		WHERE tenant_id = ?
		ORDER BY target_id, agent_type, id
	`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var agents []SNMPAgentConfig
	for rows.Next() {
		var agent SNMPAgentConfig
		var lastSeen, lastRun, lastSuccess sql.NullTime
		var lastError sql.NullString
		if err := rows.Scan(
			&agent.ID, &agent.TenantID, &agent.TargetID, &agent.AgentType, &agent.Mode, &agent.Endpoint, &agent.TokenHash, &agent.Status,
			&lastSeen, &lastRun, &lastSuccess, &lastError, &agent.RunCount, &agent.FailureCount, &agent.CreatedAt, &agent.UpdatedAt,
		); err != nil {
			return nil, err
		}
		if lastSeen.Valid {
			agent.LastSeen = lastSeen.Time
		}
		if lastRun.Valid {
			agent.LastRun = lastRun.Time
		}
		if lastSuccess.Valid {
			agent.LastSuccess = lastSuccess.Time
		}
		if lastError.Valid {
			agent.LastError = lastError.String
		}
		agents = append(agents, NormalizeAgentConfig(agent))
	}
	return agents, rows.Err()
}

func (s *MySQLStore) UpsertAgent(ctx context.Context, agent SNMPAgentConfig) (SNMPAgentConfig, error) {
	agent = NormalizeAgentConfig(agent)
	if err := ValidateAgentConfig(agent); err != nil {
		return SNMPAgentConfig{}, err
	}
	if agent.TokenHash == "" {
		return SNMPAgentConfig{}, errors.New("agent token hash is required")
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return SNMPAgentConfig{}, err
	}
	defer tx.Rollback()
	var existingTenant ID
	err = tx.QueryRowContext(ctx, `SELECT tenant_id FROM target_agents WHERE id = ? FOR UPDATE`, agent.ID).Scan(&existingTenant)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return SNMPAgentConfig{}, err
	}
	if err == nil && existingTenant != agent.TenantID {
		return SNMPAgentConfig{}, errors.New("agent id belongs to another tenant")
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO target_agents (
			id, tenant_id, target_id, agent_type, mode, endpoint, token_hash, status
		) VALUES (?, ?, ?, ?, ?, NULLIF(?, ''), ?, ?)
		ON DUPLICATE KEY UPDATE
			target_id = VALUES(target_id),
			agent_type = VALUES(agent_type),
			mode = VALUES(mode),
			endpoint = VALUES(endpoint),
			token_hash = VALUES(token_hash),
			status = VALUES(status),
			updated_at = CURRENT_TIMESTAMP(3)
	`, agent.ID, agent.TenantID, agent.TargetID, agent.AgentType, agent.Mode, agent.Endpoint, agent.TokenHash, agent.Status)
	if err != nil {
		return SNMPAgentConfig{}, err
	}
	if err := upsertCollectorCompatibilityProjection(ctx, tx, agent); err != nil {
		return SNMPAgentConfig{}, err
	}
	if err := tx.Commit(); err != nil {
		return SNMPAgentConfig{}, err
	}
	return s.GetAgent(ctx, agent.ID)
}

func (s *MySQLStore) DeleteAgent(ctx context.Context, tenantID, agentID ID) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM collector_agents
		WHERE tenant_id = ? AND id = ? AND created_by = ?
	`, tenantID, agentID, collectorCompatibilityActor); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM target_agents
		WHERE tenant_id = ? AND id = ?
	`, tenantID, agentID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *MySQLStore) MarkAgentSeen(ctx context.Context, agentID ID) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	now := time.Now().UTC()
	if _, err := tx.ExecContext(ctx, `
		UPDATE target_agents
		SET status = 'up', last_seen_at = ?, updated_at = CURRENT_TIMESTAMP(3)
		WHERE id = ?
	`, now, agentID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE collector_agents
		SET observed_health = 'healthy', last_seen_at = ?,
			last_error_code = NULL, last_error_detail = NULL,
			updated_by = ?, updated_at = CURRENT_TIMESTAMP(3)
		WHERE id = ? AND created_by = ? AND deleted_at IS NULL
	`, now, collectorCompatibilityActor, agentID, collectorCompatibilityActor); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *MySQLStore) RecordAgentRun(ctx context.Context, report AgentRunReport) error {
	if report.Status != AgentRunSuccess && report.Status != AgentRunFailure {
		return errors.New("agent run status must be success or failure")
	}
	endedAt := report.EndedAt
	if endedAt.IsZero() {
		endedAt = time.Now().UTC()
	}
	startedAt := report.StartedAt
	if startedAt.IsZero() || startedAt.After(endedAt) {
		startedAt = endedAt
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var agent SNMPAgentConfig
	if err := tx.QueryRowContext(ctx, `
		SELECT tenant_id, target_id
		FROM target_agents
		WHERE id = ?
		FOR UPDATE
	`, report.AgentID).Scan(&agent.TenantID, &agent.TargetID); err != nil {
		return err
	}
	if report.Status == AgentRunSuccess {
		_, err := tx.ExecContext(ctx, `
			UPDATE target_agents
			SET status = 'up',
				last_seen_at = IF(?, ?, last_seen_at),
				last_run_at = ?,
				last_success_at = ?,
				last_error = NULL,
				run_count = run_count + 1,
				failure_count = 0,
				updated_at = CURRENT_TIMESTAMP(3)
			WHERE id = ?
		`, report.Seen, endedAt, endedAt, endedAt, report.AgentID)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE collector_agents
			SET observed_health = 'healthy',
				last_seen_at = IF(?, ?, last_seen_at),
				last_error_code = NULL, last_error_detail = NULL,
				updated_by = ?, updated_at = CURRENT_TIMESTAMP(3)
			WHERE id = ? AND tenant_id = ? AND created_by = ? AND deleted_at IS NULL
		`, report.Seen, endedAt, collectorCompatibilityActor, report.AgentID, agent.TenantID, collectorCompatibilityActor); err != nil {
			return err
		}
	} else {
		_, err := tx.ExecContext(ctx, `
			UPDATE target_agents
			SET status = 'error',
				last_seen_at = IF(?, ?, last_seen_at),
				last_run_at = ?,
				last_error = NULLIF(?, ''),
				run_count = run_count + 1,
				failure_count = failure_count + 1,
				updated_at = CURRENT_TIMESTAMP(3)
			WHERE id = ?
		`, report.Seen, endedAt, endedAt, report.Error, report.AgentID)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE collector_agents
			SET observed_health = 'degraded',
				last_seen_at = IF(?, ?, last_seen_at),
				last_error_code = 'LEGACY_AGENT_RUN_FAILED',
				last_error_detail = NULLIF(LEFT(?, 1024), ''),
				updated_by = ?, updated_at = CURRENT_TIMESTAMP(3)
			WHERE id = ? AND tenant_id = ? AND created_by = ? AND deleted_at IS NULL
		`, report.Seen, endedAt, report.Error, collectorCompatibilityActor, report.AgentID, agent.TenantID, collectorCompatibilityActor); err != nil {
			return err
		}
	}
	duration := endedAt.Sub(startedAt)
	if duration < 0 {
		duration = 0
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO agent_run_history (
			id, tenant_id, agent_id, target_id, status, error, seen, started_at, ended_at, duration_ms
		) VALUES (?, ?, ?, ?, ?, NULLIF(?, ''), ?, ?, ?, ?)
	`, stableID("run", string(report.AgentID), endedAt.Format(time.RFC3339Nano)), agent.TenantID, report.AgentID, agent.TargetID, report.Status, report.Error, report.Seen, startedAt, endedAt, uint64(duration.Milliseconds())); err != nil {
		return err
	}
	return tx.Commit()
}

func upsertCollectorCompatibilityProjection(ctx context.Context, tx *sql.Tx, agent SNMPAgentConfig) error {
	if tx == nil {
		return errors.New("collector compatibility projection transaction is required")
	}
	// PLAT-03B1: the collector registry holds the sole credential write
	// authority. The legacy projection may seed credentials when it creates a
	// row, but an existing row keeps its auth_type/token_hash/
	// certificate_fingerprint and management row_version untouched, and rows
	// created by anything other than this projection are never modified.
	var existingTenant, existingCreator ID
	err := tx.QueryRowContext(ctx, `
		SELECT tenant_id, created_by FROM collector_agents WHERE id = ? FOR UPDATE
	`, agent.ID).Scan(&existingTenant, &existingCreator)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		name := fmt.Sprintf("legacy-%s-%s", agent.AgentType, agent.ID)
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO collector_agents (
				id, tenant_id, module_key, name, agent_type, mode, endpoint,
				status, observed_health, auth_type, token_hash, created_by, updated_by
			) VALUES (?, ?, ?, ?, ?, ?, NULLIF(?, ''), ?, ?, 'token', ?, ?, ?)
		`, agent.ID, agent.TenantID, legacyCollectorModuleKey(agent.AgentType), name,
			agent.AgentType, agent.Mode, agent.Endpoint, legacyCollectorDesiredStatus(agent.Status),
			legacyCollectorObservedHealth(agent.Status), agent.TokenHash,
			collectorCompatibilityActor, collectorCompatibilityActor); err != nil {
			return err
		}
	case err != nil:
		return err
	default:
		if existingTenant != agent.TenantID {
			return errors.New("collector id belongs to another tenant")
		}
		if existingCreator != collectorCompatibilityActor {
			return errors.New("collector id is owned by the collector registry; legacy agent writes cannot modify it")
		}
		if _, err := tx.ExecContext(ctx, `
			UPDATE collector_agents
			SET module_key = ?, agent_type = ?, mode = ?, endpoint = NULLIF(?, ''),
				status = ?, observed_health = ?,
				updated_by = ?, updated_at = CURRENT_TIMESTAMP(3)
			WHERE id = ? AND tenant_id = ?
		`, legacyCollectorModuleKey(agent.AgentType), agent.AgentType, agent.Mode, agent.Endpoint,
			legacyCollectorDesiredStatus(agent.Status), legacyCollectorObservedHealth(agent.Status),
			collectorCompatibilityActor, agent.ID, agent.TenantID); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM collector_bindings
		WHERE collector_id = ? AND tenant_id = ? AND resource_type = 'target'
	`, agent.ID, agent.TenantID); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `
		INSERT INTO collector_bindings (
			collector_id, tenant_id, resource_type, resource_id, binding_role,
			created_by, updated_by
		) VALUES (?, ?, 'target', ?, 'collect', ?, ?)
	`, agent.ID, agent.TenantID, agent.TargetID, collectorCompatibilityActor, collectorCompatibilityActor)
	return err
}

func legacyCollectorModuleKey(agentType AgentType) string {
	if agentType == AgentTypeSNMP {
		return "network"
	}
	return "watchdog"
}

func legacyCollectorDesiredStatus(status string) string {
	switch status {
	case AgentStatusPending:
		return "pending"
	case AgentStatusDisabled:
		return "suspended"
	default:
		return "active"
	}
}

func legacyCollectorObservedHealth(status string) string {
	switch status {
	case AgentStatusUp:
		return "healthy"
	case AgentStatusError:
		return "degraded"
	case AgentStatusDown:
		return "unavailable"
	default:
		return "unknown"
	}
}

func (s *MySQLStore) ListAgentRuns(ctx context.Context, tenantID, agentID ID, limit int) ([]AgentRunHistory, error) {
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, tenant_id, agent_id, target_id, status, COALESCE(error, ''), seen,
			started_at, ended_at, duration_ms, created_at
		FROM agent_run_history
		WHERE tenant_id = ? AND agent_id = ?
		ORDER BY ended_at DESC, id DESC
		LIMIT ?
	`, tenantID, agentID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var runs []AgentRunHistory
	for rows.Next() {
		var run AgentRunHistory
		if err := rows.Scan(
			&run.ID, &run.TenantID, &run.AgentID, &run.TargetID, &run.Status, &run.Error, &run.Seen,
			&run.StartedAt, &run.EndedAt, &run.DurationMS, &run.CreatedAt,
		); err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	return runs, rows.Err()
}

var _ AgentRepository = (*MySQLStore)(nil)
