-- Fleet Phase 2: the rollout is orchestration metadata only. Per-collector
-- plans remain authoritative in collector_plan_revisions, and later rollout
-- phases must activate them through the existing guarded repository path.

CREATE TABLE IF NOT EXISTS collector_plan_rollouts (
  id CHAR(26) COLLATE utf8mb4_unicode_ci NOT NULL,
  tenant_id CHAR(26) COLLATE utf8mb4_unicode_ci NOT NULL,
  module_key VARCHAR(64) COLLATE utf8mb4_unicode_ci NOT NULL,
  rollout_schema_version SMALLINT UNSIGNED NOT NULL DEFAULT 1,
  selector_json JSON NOT NULL,
  selector_hash CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  spec_json JSON NOT NULL,
  spec_hash CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  plan_schema_version SMALLINT UNSIGNED NOT NULL,
  strategy_json JSON NOT NULL,
  strategy_hash CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  status VARCHAR(16) CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'draft',
  expires_at DATETIME(3) NOT NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_by CHAR(26) COLLATE utf8mb4_unicode_ci NOT NULL,
  updated_by CHAR(26) COLLATE utf8mb4_unicode_ci NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
    ON UPDATE CURRENT_TIMESTAMP(3),
  previewed_at DATETIME(3) NULL,
  completed_at DATETIME(3) NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_collector_plan_rollout_tenant_id (tenant_id, id),
  KEY idx_collector_plan_rollout_status (tenant_id, status, updated_at, id),
  KEY idx_collector_plan_rollout_module (tenant_id, module_key, status, id),
  KEY fk_collector_plan_rollout_creator (created_by),
  KEY fk_collector_plan_rollout_updater (updated_by),
  CONSTRAINT fk_collector_plan_rollout_tenant
    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT fk_collector_plan_rollout_creator
    FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE RESTRICT,
  CONSTRAINT fk_collector_plan_rollout_updater
    FOREIGN KEY (updated_by) REFERENCES users(id) ON DELETE RESTRICT,
  CONSTRAINT chk_collector_plan_rollout_selector CHECK (JSON_TYPE(selector_json) = 'OBJECT'),
  CONSTRAINT chk_collector_plan_rollout_spec CHECK (JSON_TYPE(spec_json) = 'OBJECT'),
  CONSTRAINT chk_collector_plan_rollout_strategy CHECK (JSON_TYPE(strategy_json) = 'OBJECT'),
  CONSTRAINT chk_collector_plan_rollout_hashes CHECK (
    CHAR_LENGTH(selector_hash) = 64 AND CHAR_LENGTH(spec_hash) = 64
      AND CHAR_LENGTH(strategy_hash) = 64
  ),
  CONSTRAINT chk_collector_plan_rollout_schema CHECK (
    rollout_schema_version = 1 AND plan_schema_version > 0
  ),
  CONSTRAINT chk_collector_plan_rollout_status CHECK (
    status IN ('draft','previewed','canarying','rolling','paused',
      'completed','rolled_back','killed')
  ),
  CONSTRAINT chk_collector_plan_rollout_preview CHECK (
    (status = 'draft' AND previewed_at IS NULL) OR status = 'paused'
      OR (status NOT IN ('draft','paused') AND previewed_at IS NOT NULL)
  ),
  CONSTRAINT chk_collector_plan_rollout_completion CHECK (
    (status IN ('completed','rolled_back','killed')) = (completed_at IS NOT NULL)
  )
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS collector_plan_rollout_targets (
  tenant_id CHAR(26) COLLATE utf8mb4_unicode_ci NOT NULL,
  rollout_id CHAR(26) COLLATE utf8mb4_unicode_ci NOT NULL,
  collector_id CHAR(26) COLLATE utf8mb4_unicode_ci NOT NULL,
  wave INT UNSIGNED NOT NULL,
  config_version BIGINT UNSIGNED NULL,
  prior_config_version BIGINT UNSIGNED NOT NULL,
  status VARCHAR(24) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  failure_reason VARCHAR(1024) COLLATE utf8mb4_unicode_ci NULL,
  activated_at DATETIME(3) NULL,
  acked_at DATETIME(3) NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
    ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (rollout_id, collector_id),
  UNIQUE KEY uq_collector_plan_rollout_target_tenant
    (tenant_id, rollout_id, collector_id),
  KEY idx_collector_plan_rollout_target_wave
    (rollout_id, wave, status, collector_id),
  KEY idx_collector_plan_rollout_target_collector
    (tenant_id, collector_id, status, rollout_id),
  KEY fk_collector_plan_rollout_target_plan
    (tenant_id, collector_id, config_version),
  CONSTRAINT fk_collector_plan_rollout_target_rollout
    FOREIGN KEY (tenant_id, rollout_id)
    REFERENCES collector_plan_rollouts(tenant_id, id) ON DELETE CASCADE,
  CONSTRAINT fk_collector_plan_rollout_target_collector
    FOREIGN KEY (tenant_id, collector_id)
    REFERENCES collector_agents(tenant_id, id) ON DELETE RESTRICT,
  CONSTRAINT fk_collector_plan_rollout_target_plan
    FOREIGN KEY (tenant_id, collector_id, config_version)
    REFERENCES collector_plan_revisions(tenant_id, collector_id, config_version)
    ON DELETE RESTRICT,
  CONSTRAINT chk_collector_plan_rollout_target_status CHECK (
    status IN ('pending','revision_created','activated','acked','failed',
      'reverted','skipped')
  ),
  CONSTRAINT chk_collector_plan_rollout_target_version CHECK (
    (status IN ('pending','skipped') AND config_version IS NULL)
      OR (status IN ('revision_created','activated','acked','failed','reverted')
        AND config_version IS NOT NULL)
  ),
  CONSTRAINT chk_collector_plan_rollout_target_reason CHECK (
    (status IN ('failed','skipped') AND failure_reason IS NOT NULL)
      OR status NOT IN ('failed','skipped')
  ),
  CONSTRAINT chk_collector_plan_rollout_target_times CHECK (
    acked_at IS NULL OR (activated_at IS NOT NULL AND config_version IS NOT NULL)
  )
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
