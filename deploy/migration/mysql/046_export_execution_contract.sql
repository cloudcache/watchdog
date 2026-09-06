-- PLAT P2 provider-neutral export execution. export_tasks remains the domain
-- record and operation_jobs owns lease/retry/cancel. Legacy rows are backfilled
-- as contract_version=0 with explicit incomplete version evidence; new code
-- writes contract_version=1 immutable query/version/authorization snapshots.
SET @schema_name = DATABASE();

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = 'export_tasks' AND column_name = 'contract_version') = 0,
  'ALTER TABLE export_tasks ADD COLUMN contract_version SMALLINT UNSIGNED NOT NULL DEFAULT 0 AFTER created_by',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = 'export_tasks' AND column_name = 'dataset_key') = 0,
  'ALTER TABLE export_tasks ADD COLUMN dataset_key VARCHAR(128) NOT NULL DEFAULT ''network.snmp_interface'' AFTER contract_version',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = 'export_tasks' AND column_name = 'query_json') = 0,
  'ALTER TABLE export_tasks ADD COLUMN query_json JSON NULL AFTER dataset_key',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

UPDATE export_tasks
SET query_json = JSON_OBJECT(
  'schema_version', 0,
  'legacy', TRUE,
  'target_id', COALESCE(target_id, ''),
  'port_id', COALESCE(port_id, ''),
  'range_start', CAST(range_start AS CHAR),
  'range_end', CAST(range_end AS CHAR),
  'step_seconds', step_seconds,
  'aggregation', aggregation,
  'legacy_value_mode', value_mode
)
WHERE query_json IS NULL;

ALTER TABLE export_tasks MODIFY COLUMN query_json JSON NOT NULL;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = 'export_tasks' AND column_name = 'query_hash') = 0,
  'ALTER TABLE export_tasks ADD COLUMN query_hash CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL AFTER query_json',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

UPDATE export_tasks
SET query_hash = SHA2(CAST(query_json AS CHAR), 256)
WHERE query_hash IS NULL OR query_hash = '';

ALTER TABLE export_tasks MODIFY COLUMN query_hash CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = 'export_tasks' AND column_name = 'value_layer') = 0,
  'ALTER TABLE export_tasks ADD COLUMN value_layer VARCHAR(16) NOT NULL DEFAULT ''customer'' AFTER query_hash',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

UPDATE export_tasks
SET value_layer = CASE WHEN value_mode = 'raw' THEN 'raw' ELSE 'customer' END
WHERE contract_version = 0;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = 'export_tasks' AND column_name = 'versions_json') = 0,
  'ALTER TABLE export_tasks ADD COLUMN versions_json JSON NULL AFTER value_layer',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

UPDATE export_tasks
SET versions_json = JSON_OBJECT(
  'dataset_descriptor', 'legacy-v0',
  'query_policy', 'unknown',
  'snapshot_complete', FALSE
)
WHERE versions_json IS NULL;

ALTER TABLE export_tasks MODIFY COLUMN versions_json JSON NOT NULL;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = 'export_tasks' AND column_name = 'authorization_json') = 0,
  'ALTER TABLE export_tasks ADD COLUMN authorization_json JSON NULL AFTER versions_json',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

UPDATE export_tasks
SET authorization_json = JSON_OBJECT(
  'subject_id', created_by,
  'required_action', CASE WHEN value_mode IN ('raw', 'both') THEN 'export_raw' ELSE 'export_customer' END,
  'resource_type', CASE WHEN port_id IS NULL THEN 'target' ELSE 'port' END,
  'resource_id', COALESCE(port_id, target_id, ''),
  'legacy', TRUE
)
WHERE authorization_json IS NULL;

ALTER TABLE export_tasks MODIFY COLUMN authorization_json JSON NOT NULL;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = 'export_tasks' AND column_name = 'operation_job_id') = 0,
  'ALTER TABLE export_tasks ADD COLUMN operation_job_id CHAR(26) NULL AFTER authorization_json',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = 'export_tasks' AND column_name = 'retention_seconds') = 0,
  'ALTER TABLE export_tasks ADD COLUMN retention_seconds INT UNSIGNED NOT NULL DEFAULT 604800 AFTER operation_job_id',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = 'export_tasks' AND column_name = 'artifact_schema_version') = 0,
  'ALTER TABLE export_tasks ADD COLUMN artifact_schema_version SMALLINT UNSIGNED NULL AFTER retention_seconds',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

UPDATE export_tasks
SET artifact_schema_version = 1
WHERE status = 'complete' AND artifact_schema_version IS NULL;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = 'export_tasks' AND column_name = 'content_type') = 0,
  'ALTER TABLE export_tasks ADD COLUMN content_type VARCHAR(128) NULL AFTER artifact_schema_version',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

UPDATE export_tasks
SET content_type = 'text/csv; charset=utf-8'
WHERE status = 'complete' AND format = 'csv' AND content_type IS NULL;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = 'export_tasks' AND column_name = 'row_count') = 0,
  'ALTER TABLE export_tasks ADD COLUMN row_count BIGINT UNSIGNED NULL AFTER content_type',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = 'export_tasks' AND column_name = 'row_version') = 0,
  'ALTER TABLE export_tasks ADD COLUMN row_version BIGINT UNSIGNED NOT NULL DEFAULT 1 AFTER error_message',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = @schema_name AND table_name = 'export_tasks' AND index_name = 'uq_export_tasks_operation_job') = 0,
  'ALTER TABLE export_tasks ADD UNIQUE KEY uq_export_tasks_operation_job (operation_job_id)',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.table_constraints WHERE table_schema = @schema_name AND table_name = 'export_tasks' AND constraint_name = 'fk_export_tasks_operation_job') = 0,
  'ALTER TABLE export_tasks ADD CONSTRAINT fk_export_tasks_operation_job FOREIGN KEY (operation_job_id) REFERENCES operation_jobs(id) ON DELETE SET NULL',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.table_constraints WHERE table_schema = @schema_name AND table_name = 'export_tasks' AND constraint_name = 'export_tasks_chk_4') = 1,
  'ALTER TABLE export_tasks DROP CHECK export_tasks_chk_4',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.table_constraints WHERE table_schema = @schema_name AND table_name = 'export_tasks' AND constraint_name = 'chk_export_tasks_format_v2') = 0,
  'ALTER TABLE export_tasks ADD CONSTRAINT chk_export_tasks_format_v2 CHECK (format IN (''csv'', ''parquet''))',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.table_constraints WHERE table_schema = @schema_name AND table_name = 'export_tasks' AND constraint_name = 'chk_export_tasks_value_layer') = 0,
  'ALTER TABLE export_tasks ADD CONSTRAINT chk_export_tasks_value_layer CHECK (value_layer IN (''raw'', ''supplier'', ''customer''))',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.table_constraints WHERE table_schema = @schema_name AND table_name = 'export_tasks' AND constraint_name = 'chk_export_tasks_retention') = 0,
  'ALTER TABLE export_tasks ADD CONSTRAINT chk_export_tasks_retention CHECK (retention_seconds BETWEEN 3600 AND 31536000)',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
