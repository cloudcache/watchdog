package watchdog

import (
	"context"
	"database/sql"
	"time"
)

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
	return agent, err
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
		agents = append(agents, agent)
	}
	return agents, rows.Err()
}

func (s *MySQLStore) UpsertAgent(ctx context.Context, agent SNMPAgentConfig) (SNMPAgentConfig, error) {
	agent.AgentType = normalizeAgentType(agent.AgentType)
	if agent.Status == "" {
		agent.Status = "pending"
	}
	_, err := s.db.ExecContext(ctx, `
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
	return s.GetAgent(ctx, agent.ID)
}

func (s *MySQLStore) DeleteAgent(ctx context.Context, tenantID, agentID ID) error {
	_, err := s.db.ExecContext(ctx, `
		DELETE FROM target_agents
		WHERE tenant_id = ? AND id = ?
	`, tenantID, agentID)
	return err
}

func (s *MySQLStore) MarkAgentSeen(ctx context.Context, agentID ID) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE target_agents
		SET status = 'up', last_seen_at = ?, updated_at = CURRENT_TIMESTAMP(3)
		WHERE id = ?
	`, time.Now().UTC(), agentID)
	return err
}

func (s *MySQLStore) RecordAgentRun(ctx context.Context, report AgentRunReport) error {
	endedAt := report.EndedAt
	if endedAt.IsZero() {
		endedAt = time.Now().UTC()
	}
	startedAt := report.StartedAt
	if startedAt.IsZero() || startedAt.After(endedAt) {
		startedAt = endedAt
	}
	var agent SNMPAgentConfig
	agentErr := s.db.QueryRowContext(ctx, `
		SELECT tenant_id, target_id
		FROM target_agents
		WHERE id = ?
	`, report.AgentID).Scan(&agent.TenantID, &agent.TargetID)
	if report.Status == AgentRunSuccess {
		_, err := s.db.ExecContext(ctx, `
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
	} else {
		_, err := s.db.ExecContext(ctx, `
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
	}
	if agentErr != nil {
		return agentErr
	}
	duration := endedAt.Sub(startedAt)
	if duration < 0 {
		duration = 0
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO agent_run_history (
			id, tenant_id, agent_id, target_id, status, error, seen, started_at, ended_at, duration_ms
		) VALUES (?, ?, ?, ?, ?, NULLIF(?, ''), ?, ?, ?, ?)
	`, stableID("run", string(report.AgentID), endedAt.Format(time.RFC3339Nano)), agent.TenantID, report.AgentID, agent.TargetID, report.Status, report.Error, report.Seen, startedAt, endedAt, uint64(duration.Milliseconds()))
	return err
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

func normalizeAgentType(agentType AgentType) AgentType {
	if agentType == "" {
		return AgentTypeSNMP
	}
	return agentType
}
