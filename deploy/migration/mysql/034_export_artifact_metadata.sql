-- PLAT P2 export hardening: record artifact integrity + lifecycle metadata on
-- export_tasks — a sha256 checksum and byte size of the produced file, and an
-- expiry after which the file is stale (enforced on download and reaped).
SET @schema_name = DATABASE();

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = 'export_tasks' AND column_name = 'checksum') = 0,
  'ALTER TABLE export_tasks ADD COLUMN checksum CHAR(64) NULL AFTER file_ref',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = 'export_tasks' AND column_name = 'size_bytes') = 0,
  'ALTER TABLE export_tasks ADD COLUMN size_bytes BIGINT UNSIGNED NULL AFTER checksum',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = 'export_tasks' AND column_name = 'expires_at') = 0,
  'ALTER TABLE export_tasks ADD COLUMN expires_at DATETIME(3) NULL AFTER size_bytes',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = @schema_name AND table_name = 'export_tasks' AND index_name = 'idx_export_tasks_expires') = 0,
  'ALTER TABLE export_tasks ADD KEY idx_export_tasks_expires (expires_at)',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
