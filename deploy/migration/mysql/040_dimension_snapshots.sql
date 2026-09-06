-- PLAT-04A: immutable, event-time address dimension publications. Editable
-- address tables remain draft state; workers consume only checksummed objects.
CREATE TABLE IF NOT EXISTS dimension_snapshots (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  module_key VARCHAR(64) NOT NULL,
  dimension_key VARCHAR(64) NOT NULL,
  version BIGINT UNSIGNED NOT NULL,
  effective_from DATETIME(3) NOT NULL,
  object_ref VARCHAR(512) NOT NULL,
  checksum VARCHAR(128) NOT NULL,
  draft_digest CHAR(71) NOT NULL,
  bundle_schema_version INT UNSIGNED NOT NULL,
  entry_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
  prefix_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
  address_set_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
  max_address_sets_per_record INT UNSIGNED NOT NULL DEFAULT 0,
  status VARCHAR(16) NOT NULL DEFAULT 'active',
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_by CHAR(26) NULL,
  retired_by CHAR(26) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  retired_at DATETIME(3) NULL,
  UNIQUE KEY uq_dimension_snapshot_tenant_id (tenant_id, id),
  UNIQUE KEY uq_dimension_version (tenant_id, module_key, dimension_key, version),
  UNIQUE KEY uq_dimension_effective (tenant_id, module_key, dimension_key, effective_from),
  KEY idx_dimension_effective (tenant_id, module_key, dimension_key, status, effective_from),
  CONSTRAINT fk_dimension_snapshot_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT fk_dimension_snapshot_creator FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL,
  CONSTRAINT fk_dimension_snapshot_retired_by FOREIGN KEY (retired_by) REFERENCES users(id) ON DELETE SET NULL,
  CHECK (status IN ('active','retired')),
  CHECK (version > 0),
  CHECK (bundle_schema_version > 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS dimension_snapshot_acks (
  tenant_id CHAR(26) NOT NULL,
  snapshot_id CHAR(26) NOT NULL,
  worker_id VARCHAR(128) NOT NULL,
  boot_id VARCHAR(128) NOT NULL,
  software_version VARCHAR(64) NOT NULL,
  checksum VARCHAR(128) NOT NULL,
  installed_at DATETIME(3) NOT NULL,
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (tenant_id, snapshot_id, worker_id),
  KEY idx_dimension_snapshot_acks_worker (tenant_id, worker_id, installed_at),
  CONSTRAINT fk_dimension_snapshot_acks_snapshot FOREIGN KEY (tenant_id, snapshot_id)
    REFERENCES dimension_snapshots(tenant_id, id) ON DELETE CASCADE,
  CONSTRAINT fk_dimension_snapshot_acks_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
