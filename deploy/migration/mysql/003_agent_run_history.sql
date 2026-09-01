CREATE TABLE IF NOT EXISTS agent_run_history (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  agent_id CHAR(26) NOT NULL,
  target_id CHAR(26) NOT NULL,
  status VARCHAR(16) NOT NULL,
  error TEXT NULL,
  seen BOOLEAN NOT NULL DEFAULT FALSE,
  started_at DATETIME(3) NOT NULL,
  ended_at DATETIME(3) NOT NULL,
  duration_ms BIGINT UNSIGNED NOT NULL DEFAULT 0,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  KEY idx_agent_run_history_agent (tenant_id, agent_id, ended_at),
  KEY idx_agent_run_history_status (tenant_id, status, ended_at),
  CONSTRAINT fk_agent_run_history_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT fk_agent_run_history_agent FOREIGN KEY (agent_id) REFERENCES target_agents(id) ON DELETE CASCADE,
  CONSTRAINT fk_agent_run_history_target FOREIGN KEY (target_id) REFERENCES monitor_targets(id) ON DELETE CASCADE,
  CHECK (status IN ('success', 'failure'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
