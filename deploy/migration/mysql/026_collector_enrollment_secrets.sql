-- PLAT-03C2: one-time enrollment secrets. An admin stages the collector's
-- identity (name/module/type/mode) and receives a single-use secret; the
-- collector exchanges the secret exactly once for its registry row and
-- initial token. Consumption is atomic (used_at flips under row lock), so a
-- stolen-and-replayed secret cannot mint a second collector.

CREATE TABLE IF NOT EXISTS collector_enrollment_secrets (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  secret_hash VARCHAR(255) NOT NULL,
  module_key VARCHAR(64) NOT NULL,
  agent_type VARCHAR(64) NOT NULL,
  mode VARCHAR(16) NOT NULL,
  collector_name VARCHAR(190) NOT NULL,
  expires_at DATETIME(3) NOT NULL,
  used_at DATETIME(3) NULL,
  used_by_collector_id CHAR(26) NULL,
  created_by CHAR(26) NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  KEY idx_collector_enrollment_tenant (tenant_id, expires_at),
  CONSTRAINT fk_collector_enrollment_tenant
    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CHECK ((used_at IS NULL) = (used_by_collector_id IS NULL)),
  CHECK (mode IN ('push','pull','listen'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
