CREATE TABLE IF NOT EXISTS operation_jobs (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  job_type VARCHAR(64) NOT NULL,
  status VARCHAR(16) NOT NULL DEFAULT 'queued',
  idempotency_key VARCHAR(128) NOT NULL,
  request_hash CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  progress_total BIGINT UNSIGNED NULL,
  progress_done BIGINT UNSIGNED NOT NULL DEFAULT 0,
  checkpoint_json JSON NOT NULL,
  result_ref VARCHAR(1024) NULL,
  lease_owner VARCHAR(128) NULL,
  lease_token VARCHAR(64) NULL,
  lease_expires_at DATETIME(3) NULL,
  next_attempt_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  attempt_count INT UNSIGNED NOT NULL DEFAULT 0,
  last_error_code VARCHAR(64) NULL,
  last_error_detail VARCHAR(1024) NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_by CHAR(26) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  started_at DATETIME(3) NULL,
  heartbeat_at DATETIME(3) NULL,
  cancel_requested_at DATETIME(3) NULL,
  finished_at DATETIME(3) NULL,
  expires_at DATETIME(3) NULL,
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
    ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_operation_jobs_idempotency
    (tenant_id, job_type, idempotency_key),
  KEY idx_operation_jobs_due
    (job_type, status, next_attempt_at, lease_expires_at, id),
  KEY idx_operation_jobs_tenant_created (tenant_id, job_type, created_at, id),
  CONSTRAINT fk_operation_jobs_tenant
    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT fk_operation_jobs_creator
    FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL,
  CHECK (status IN ('queued','running','paused','validating','cancel_requested','succeeded','failed','canceled')),
  CHECK (progress_total IS NULL OR progress_done <= progress_total),
  CHECK ((lease_owner IS NULL) = (lease_token IS NULL)),
  CHECK ((lease_owner IS NULL) = (lease_expires_at IS NULL)),
  CHECK ((status IN ('running','validating','cancel_requested')) =
    (lease_owner IS NOT NULL)),
  CHECK ((status IN ('succeeded','failed','canceled')) =
    (finished_at IS NOT NULL))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
