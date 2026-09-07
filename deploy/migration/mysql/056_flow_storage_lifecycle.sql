-- Flow Storage V2 control-plane policy and per-tenant UTC-day state. Raw and
-- archive deletion is never inferred from a ClickHouse TTL: operation_jobs
-- moves a day through explicit, auditable states under one immutable policy.
CREATE TABLE IF NOT EXISTS flow_storage_policy_revisions (
  id CHAR(26) NOT NULL,
  tenant_id CHAR(26) NOT NULL,
  policy_version BIGINT UNSIGNED NOT NULL,
  status VARCHAR(16) NOT NULL DEFAULT 'draft',
  bootstrap_from DATE NOT NULL,
  raw_retention_seconds BIGINT UNSIGNED NOT NULL,
  downsample_resolution_seconds INT UNSIGNED NOT NULL DEFAULT 3600,
  archive_retention_seconds BIGINT UNSIGNED NOT NULL DEFAULT 0,
  late_arrival_seconds INT UNSIGNED NOT NULL,
  delete_grace_seconds INT UNSIGNED NOT NULL,
  max_partitions_per_run INT UNSIGNED NOT NULL,
  raw_delete_enabled TINYINT(1) NOT NULL DEFAULT 0,
  published_tenant_id CHAR(26)
    GENERATED ALWAYS AS (CASE WHEN status = 'published' THEN tenant_id ELSE NULL END) VIRTUAL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_by CHAR(26) NULL,
  published_by CHAR(26) NULL,
  retired_by CHAR(26) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  published_at DATETIME(3) NULL,
  retired_at DATETIME(3) NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_flow_storage_policy_tenant_id (tenant_id, id),
  UNIQUE KEY uq_flow_storage_policy_version (tenant_id, policy_version),
  UNIQUE KEY uq_flow_storage_policy_published (published_tenant_id),
  KEY idx_flow_storage_policy_list (tenant_id, status, policy_version),
  CONSTRAINT fk_flow_storage_policy_tenant
    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT fk_flow_storage_policy_creator
    FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL,
  CONSTRAINT fk_flow_storage_policy_publisher
    FOREIGN KEY (published_by) REFERENCES users(id) ON DELETE SET NULL,
  CONSTRAINT fk_flow_storage_policy_retirer
    FOREIGN KEY (retired_by) REFERENCES users(id) ON DELETE SET NULL,
  CHECK (status IN ('draft', 'published', 'retired')),
  CHECK (policy_version BETWEEN 1 AND 4294967295),
  CHECK (raw_retention_seconds BETWEEN 86400 AND 315576000),
  CHECK (downsample_resolution_seconds = 3600),
  CHECK (archive_retention_seconds = 0 OR archive_retention_seconds > raw_retention_seconds),
  CHECK (late_arrival_seconds <= 604800),
  CHECK (delete_grace_seconds BETWEEN 3600 AND 2592000),
  CHECK (max_partitions_per_run BETWEEN 1 AND 366),
  CHECK (raw_delete_enabled IN (0, 1)),
  CHECK ((status = 'draft' AND published_at IS NULL AND retired_at IS NULL)
      OR (status = 'published' AND published_at IS NOT NULL AND retired_at IS NULL)
      OR (status = 'retired' AND published_at IS NOT NULL AND retired_at IS NOT NULL))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS flow_storage_partition_states (
  tenant_id CHAR(26) NOT NULL,
  source_date DATE NOT NULL,
  policy_id CHAR(26) NOT NULL,
  policy_version BIGINT UNSIGNED NOT NULL,
  state VARCHAR(24) NOT NULL DEFAULT 'sealed',
  generation BIGINT UNSIGNED NOT NULL DEFAULT 1,
  source_record_count BIGINT UNSIGNED NULL,
  source_raw_bytes BIGINT UNSIGNED NULL,
  source_raw_packets BIGINT UNSIGNED NULL,
  source_estimated_bytes BIGINT UNSIGNED NULL,
  source_estimated_packets BIGINT UNSIGNED NULL,
  source_estimated_valid_records BIGINT UNSIGNED NULL,
  archive_record_count BIGINT UNSIGNED NULL,
  archive_raw_bytes BIGINT UNSIGNED NULL,
  archive_raw_packets BIGINT UNSIGNED NULL,
  archive_estimated_bytes BIGINT UNSIGNED NULL,
  archive_estimated_packets BIGINT UNSIGNED NULL,
  archive_estimated_valid_records BIGINT UNSIGNED NULL,
  downsample_job_id CHAR(26) NULL,
  delete_job_id CHAR(26) NULL,
  downsampled_at DATETIME(3) NULL,
  reconciled_at DATETIME(3) NULL,
  delete_eligible_at DATETIME(3) NULL,
  raw_deleted_at DATETIME(3) NULL,
  last_error_code VARCHAR(64) NULL,
  last_error_detail VARCHAR(1024) NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (tenant_id, source_date, policy_version),
  KEY idx_flow_storage_partition_state (tenant_id, state, source_date),
  KEY idx_flow_storage_partition_delete (state, delete_eligible_at, tenant_id, source_date),
  KEY idx_flow_storage_partition_downsample_job (downsample_job_id),
  KEY idx_flow_storage_partition_delete_job (delete_job_id),
  CONSTRAINT fk_flow_storage_partition_tenant
    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT fk_flow_storage_partition_policy
    FOREIGN KEY (tenant_id, policy_id) REFERENCES flow_storage_policy_revisions(tenant_id, id) ON DELETE RESTRICT,
  CONSTRAINT fk_flow_storage_partition_downsample_job
    FOREIGN KEY (downsample_job_id) REFERENCES operation_jobs(id) ON DELETE SET NULL,
  CONSTRAINT fk_flow_storage_partition_delete_job
    FOREIGN KEY (delete_job_id) REFERENCES operation_jobs(id) ON DELETE SET NULL,
  CHECK (state IN ('sealed', 'downsample_written', 'reconciled', 'delete_eligible', 'raw_deleted', 'failed')),
  CHECK (generation > 0),
  CHECK ((state IN ('sealed', 'downsample_written', 'failed') AND raw_deleted_at IS NULL)
      OR (state IN ('reconciled', 'delete_eligible') AND reconciled_at IS NOT NULL AND raw_deleted_at IS NULL)
      OR (state = 'raw_deleted' AND reconciled_at IS NOT NULL AND raw_deleted_at IS NOT NULL))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
