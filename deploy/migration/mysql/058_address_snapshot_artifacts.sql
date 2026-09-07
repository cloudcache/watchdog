-- PLAT-04C4b2: distinguish legacy JSON definition bundles from compiled
-- AddressSnap objects and fence an asynchronous build to one operation job.
SET @schema_name = DATABASE();

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = 'dimension_snapshots' AND column_name = 'object_format') = 0,
  'ALTER TABLE dimension_snapshots ADD COLUMN object_format VARCHAR(16) NOT NULL DEFAULT ''json'' AFTER object_ref',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Supplier ISP identities are exact normalized names from the pinned source,
-- not customer mappings and not fuzzy aliases. IDs are monotonic and never
-- reused, so facts remain interpretable across AddressSnap generations.
CREATE TABLE IF NOT EXISTS address_supplier_operator_sequences (
  tenant_id CHAR(26) PRIMARY KEY,
  next_flow_isp_id INT UNSIGNED NOT NULL DEFAULT 1,
  CONSTRAINT fk_address_supplier_operator_sequence_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CHECK (next_flow_isp_id BETWEEN 1 AND 65536)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS address_supplier_operators (
  tenant_id CHAR(26) NOT NULL,
  supplier_key VARCHAR(190) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin NOT NULL,
  flow_isp_id SMALLINT UNSIGNED NOT NULL,
  name VARCHAR(190) NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (tenant_id, supplier_key),
  UNIQUE KEY uq_address_supplier_operator_flow_id (tenant_id, flow_isp_id),
  CONSTRAINT fk_address_supplier_operator_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CHECK (flow_isp_id > 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = 'dimension_snapshots' AND column_name = 'object_format_version') = 0,
  'ALTER TABLE dimension_snapshots ADD COLUMN object_format_version SMALLINT UNSIGNED NOT NULL DEFAULT 0 AFTER object_format',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = 'dimension_snapshots' AND column_name = 'builder_version') = 0,
  'ALTER TABLE dimension_snapshots ADD COLUMN builder_version VARCHAR(64) NOT NULL DEFAULT '''' AFTER object_format_version',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = 'dimension_snapshots' AND column_name = 'build_job_id') = 0,
  'ALTER TABLE dimension_snapshots ADD COLUMN build_job_id CHAR(26) NULL AFTER builder_version',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

UPDATE dimension_snapshots
SET object_format_version = bundle_schema_version
WHERE object_format = 'json' AND object_format_version = 0;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = @schema_name AND table_name = 'dimension_snapshots' AND index_name = 'uq_dimension_snapshot_build_job') = 0,
  'ALTER TABLE dimension_snapshots ADD UNIQUE KEY uq_dimension_snapshot_build_job (tenant_id, module_key, dimension_key, build_job_id)',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.table_constraints WHERE constraint_schema = @schema_name AND table_name = 'dimension_snapshots' AND constraint_name = 'chk_dimension_snapshot_object_format') = 0,
  'ALTER TABLE dimension_snapshots
     ADD CONSTRAINT chk_dimension_snapshot_object_format CHECK (
       object_format = ''json''
       OR
       (object_format = ''wads'' AND object_format_version = 1 AND builder_version <> '''' AND build_job_id IS NOT NULL)
     )',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
