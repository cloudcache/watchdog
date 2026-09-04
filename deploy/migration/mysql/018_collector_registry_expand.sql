-- Expand the target-bound legacy agent model into the shared collector
-- registry. target_agents remains a compatibility projection until every
-- legacy SNMP/system caller and agent_run_history FK has moved.

CREATE TABLE IF NOT EXISTS collector_agents (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  module_key VARCHAR(64) NOT NULL,
  name VARCHAR(190) NOT NULL,
  agent_type VARCHAR(64) NOT NULL,
  mode VARCHAR(16) NOT NULL,
  endpoint VARCHAR(512) NULL,
  status VARCHAR(24) NOT NULL DEFAULT 'pending',
  observed_health VARCHAR(24) NOT NULL DEFAULT 'unknown',
  auth_type VARCHAR(16) NOT NULL DEFAULT 'token',
  token_hash VARCHAR(255) NULL,
  certificate_fingerprint VARCHAR(190) NULL,
  capabilities_json JSON NULL,
  capabilities_hash CHAR(64) NOT NULL DEFAULT '',
  capability_schema_version SMALLINT UNSIGNED NOT NULL DEFAULT 1,
  software_version VARCHAR(64) NOT NULL DEFAULT '',
  agent_api_version SMALLINT UNSIGNED NOT NULL DEFAULT 1,
  plan_schema_min SMALLINT UNSIGNED NOT NULL DEFAULT 1,
  plan_schema_max SMALLINT UNSIGNED NOT NULL DEFAULT 1,
  boot_id VARCHAR(64) NOT NULL DEFAULT '',
  config_version BIGINT UNSIGNED NOT NULL DEFAULT 0,
  acknowledged_config_version BIGINT UNSIGNED NOT NULL DEFAULT 0,
  last_good_config_version BIGINT UNSIGNED NOT NULL DEFAULT 0,
  plan_hash CHAR(64) NOT NULL DEFAULT '',
  plan_expires_at DATETIME(3) NULL,
  last_seen_at DATETIME(3) NULL,
  last_error_code VARCHAR(64) NULL,
  last_error_detail VARCHAR(1024) NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_by CHAR(26) NOT NULL,
  updated_by CHAR(26) NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
    ON UPDATE CURRENT_TIMESTAMP(3),
  deleted_at DATETIME(3) NULL,
  purge_after DATETIME(3) NULL,
  live_identity TINYINT GENERATED ALWAYS AS
    (IF(deleted_at IS NULL, 1, NULL)) STORED,
  UNIQUE KEY uq_collector_agent_tenant_id (tenant_id, id),
  UNIQUE KEY uq_collector_name (tenant_id, module_key, name, live_identity),
  KEY idx_collector_health (tenant_id, status, observed_health, last_seen_at),
  CONSTRAINT fk_collector_agent_tenant
    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CHECK (mode IN ('push','pull','listen')),
  CHECK (status IN ('pending','active','suspended','revoked','deleted')),
  CHECK ((status = 'deleted') = (deleted_at IS NOT NULL)),
  CHECK (observed_health IN (
    'unknown','warming','healthy','degraded','stale','unavailable'
  )),
  CHECK (plan_schema_min <= plan_schema_max),
  CHECK (last_good_config_version <= acknowledged_config_version),
  CHECK (acknowledged_config_version <= config_version),
  CHECK (auth_type IN ('token','mtls')),
  CHECK ((auth_type = 'token') = (token_hash IS NOT NULL)),
  CHECK ((auth_type = 'mtls') = (certificate_fingerprint IS NOT NULL))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS collector_bindings (
  collector_id CHAR(26) NOT NULL,
  tenant_id CHAR(26) NOT NULL,
  resource_type VARCHAR(64) NOT NULL,
  resource_id CHAR(26) NOT NULL,
  binding_role VARCHAR(64) NOT NULL DEFAULT 'collect',
  config_json JSON NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_by CHAR(26) NOT NULL,
  updated_by CHAR(26) NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
    ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (collector_id, resource_type, resource_id),
  KEY idx_collector_binding_resource
    (tenant_id, resource_type, resource_id, binding_role),
  CONSTRAINT fk_collector_binding_collector
    FOREIGN KEY (tenant_id, collector_id)
    REFERENCES collector_agents(tenant_id, id) ON DELETE CASCADE,
  CONSTRAINT fk_collector_binding_tenant
    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS collector_plan_revisions (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  collector_id CHAR(26) NOT NULL,
  config_version BIGINT UNSIGNED NOT NULL,
  plan_schema_version SMALLINT UNSIGNED NOT NULL,
  status VARCHAR(16) NOT NULL DEFAULT 'draft',
  spec_json JSON NOT NULL,
  spec_hash CHAR(64) NOT NULL,
  signing_key_id VARCHAR(64) NULL,
  signature VARBINARY(512) NULL,
  validation_json JSON NULL,
  not_before DATETIME(3) NULL,
  expires_at DATETIME(3) NULL,
  supersedes_config_version BIGINT UNSIGNED NULL,
  created_by CHAR(26) NOT NULL,
  updated_by CHAR(26) NOT NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
    ON UPDATE CURRENT_TIMESTAMP(3),
  activated_at DATETIME(3) NULL,
  retired_at DATETIME(3) NULL,
  active_identity TINYINT GENERATED ALWAYS AS
    (IF(status = 'active', 1, NULL)) STORED,
  UNIQUE KEY uq_collector_plan_version (collector_id, config_version),
  KEY idx_collector_plan_hash (collector_id, spec_hash),
  KEY idx_collector_plan_status (collector_id, status, config_version),
  UNIQUE KEY uq_collector_active_plan (collector_id, active_identity),
  CONSTRAINT fk_collector_plan_collector
    FOREIGN KEY (tenant_id, collector_id)
    REFERENCES collector_agents(tenant_id, id) ON DELETE CASCADE,
  CONSTRAINT fk_collector_plan_tenant
    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CHECK (status IN ('draft','validated','active','retired','rejected')),
  CHECK (config_version > 0),
  CHECK (plan_schema_version > 0),
  CHECK (status <> 'active' OR (
    signature IS NOT NULL AND activated_at IS NOT NULL AND expires_at IS NOT NULL
  )),
  CHECK (status <> 'retired' OR retired_at IS NOT NULL),
  CHECK (expires_at IS NULL OR not_before IS NULL OR expires_at > not_before),
  CHECK (supersedes_config_version IS NULL OR supersedes_config_version < config_version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- The compatibility actor is deliberately not a user FK: legacy workers do
-- not carry a human actor. Their API request remains represented in audit_logs.
INSERT INTO collector_agents (
  id, tenant_id, module_key, name, agent_type, mode, endpoint, status,
  observed_health, auth_type, token_hash, last_seen_at, last_error_code,
  last_error_detail, created_by, updated_by, created_at, updated_at
)
SELECT
  legacy.id,
  legacy.tenant_id,
  CASE legacy.agent_type WHEN 'snmp' THEN 'network' ELSE 'watchdog' END,
  LEFT(CONCAT('legacy-', legacy.agent_type, '-', legacy.id), 190),
  legacy.agent_type,
  legacy.mode,
  legacy.endpoint,
  CASE legacy.status
    WHEN 'pending' THEN 'pending'
    WHEN 'disabled' THEN 'suspended'
    ELSE 'active'
  END,
  CASE legacy.status
    WHEN 'up' THEN 'healthy'
    WHEN 'error' THEN 'degraded'
    WHEN 'down' THEN 'unavailable'
    ELSE 'unknown'
  END,
  'token',
  legacy.token_hash,
  legacy.last_seen_at,
  CASE WHEN legacy.status = 'error' THEN 'LEGACY_AGENT_RUN_FAILED' ELSE NULL END,
  LEFT(legacy.last_error, 1024),
  'system:migration',
  'system:migration',
  legacy.created_at,
  legacy.updated_at
FROM target_agents AS legacy
ON DUPLICATE KEY UPDATE id = VALUES(id);

INSERT INTO collector_bindings (
  collector_id, tenant_id, resource_type, resource_id, binding_role,
  created_by, updated_by, created_at, updated_at
)
SELECT
  legacy.id,
  legacy.tenant_id,
  'target',
  legacy.target_id,
  'collect',
  'system:migration',
  'system:migration',
  legacy.created_at,
  legacy.updated_at
FROM target_agents AS legacy
ON DUPLICATE KEY UPDATE collector_id = VALUES(collector_id);
