-- PLAT-04B/04F operation scheduler. Schedules are management-plane triggers;
-- the resulting work still executes only through operation_jobs leases. The
-- explicit scope correspondence matches operation_jobs and never fabricates a
-- system tenant. schedule_id makes inflight/backpressure checks exact instead
-- of parsing idempotency keys. The singleton scanner state persists the fair
-- wrap-around cursor across hub failover. System watermarks remain separate
-- from the tenant table so its tenant FK and cascade semantics stay intact.

CREATE TABLE IF NOT EXISTS operation_job_schedules (
  id CHAR(26) COLLATE utf8mb4_unicode_ci NOT NULL,
  tenant_id CHAR(26) COLLATE utf8mb4_unicode_ci NULL,
  scope_type VARCHAR(16) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'tenant',
  idempotency_domain CHAR(26) COLLATE utf8mb4_unicode_ci
    GENERATED ALWAYS AS (COALESCE(tenant_id, '__system__')) VIRTUAL,
  name VARCHAR(190) COLLATE utf8mb4_unicode_ci NOT NULL,
  job_type VARCHAR(64) COLLATE utf8mb4_unicode_ci NOT NULL,
  partition_key VARCHAR(128) COLLATE utf8mb4_unicode_ci NOT NULL,
  cron_expression VARCHAR(128) COLLATE utf8mb4_unicode_ci NOT NULL,
  timezone VARCHAR(64) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT 'UTC',
  payload_json JSON NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  max_inflight INT UNSIGNED NOT NULL DEFAULT 1,
  next_run_at DATETIME(3) NOT NULL,
  last_enqueued_at DATETIME(3) NULL,
  last_error_detail VARCHAR(1024) COLLATE utf8mb4_unicode_ci NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_by CHAR(26) COLLATE utf8mb4_unicode_ci NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_operation_job_schedule_domain (idempotency_domain, job_type, partition_key),
  KEY idx_operation_job_schedules_due (enabled, next_run_at, id),
  KEY idx_operation_job_schedules_tenant (tenant_id, created_at, id),
  KEY fk_operation_job_schedules_creator (created_by),
  CONSTRAINT fk_operation_job_schedules_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT fk_operation_job_schedules_creator FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL,
  CONSTRAINT chk_operation_job_schedule_scope CHECK (
    (scope_type = 'tenant' AND tenant_id IS NOT NULL) OR
    (scope_type = 'system' AND tenant_id IS NULL)
  ),
  CONSTRAINT chk_operation_job_schedule_inflight CHECK (max_inflight BETWEEN 1 AND 1024)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS operation_job_scheduler_state (
  scanner_key VARCHAR(64) COLLATE utf8mb4_unicode_ci NOT NULL,
  cursor_due_at DATETIME(3) NULL,
  cursor_schedule_id CHAR(26) COLLATE utf8mb4_unicode_ci NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (scanner_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS operation_job_system_watermarks (
  job_type VARCHAR(64) COLLATE utf8mb4_unicode_ci NOT NULL,
  partition_key VARCHAR(128) COLLATE utf8mb4_unicode_ci NOT NULL,
  watermark_value BIGINT UNSIGNED NOT NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (job_type, partition_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

SET @schema_name = DATABASE();

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = 'operation_jobs' AND column_name = 'schedule_id') = 0,
  'ALTER TABLE operation_jobs ADD COLUMN schedule_id CHAR(26) COLLATE utf8mb4_unicode_ci NULL AFTER scope_type',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = @schema_name AND table_name = 'operation_jobs' AND index_name = 'idx_operation_jobs_schedule_status') = 0,
  'ALTER TABLE operation_jobs ADD KEY idx_operation_jobs_schedule_status (schedule_id, status, id)',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.table_constraints WHERE table_schema = @schema_name AND table_name = 'operation_jobs' AND constraint_name = 'fk_operation_jobs_schedule' AND constraint_type = 'FOREIGN KEY') = 0,
  'ALTER TABLE operation_jobs ADD CONSTRAINT fk_operation_jobs_schedule FOREIGN KEY (schedule_id) REFERENCES operation_job_schedules(id) ON DELETE SET NULL',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
