-- PLAT-04A2b: pin active immutable MMDB/IPDB generations in every address
-- dimension snapshot. Large base generations remain in address_base_prefixes;
-- the snapshot object contains only manual overrides and groups, avoiding a
-- second million-row JSON copy. draft_digest covers this manifest.
SET @schema_name = DATABASE();

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = 'dimension_snapshots' AND column_name = 'source_manifest_version') = 0,
  'ALTER TABLE dimension_snapshots
     ADD COLUMN source_manifest_version SMALLINT UNSIGNED NOT NULL DEFAULT 0 AFTER draft_digest',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = 'dimension_snapshots' AND column_name = 'source_manifest') = 0,
  'ALTER TABLE dimension_snapshots
     ADD COLUMN source_manifest JSON NULL AFTER source_manifest_version',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = 'dimension_snapshots' AND column_name = 'source_prefix_count') = 0,
  'ALTER TABLE dimension_snapshots
     ADD COLUMN source_prefix_count BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER source_manifest',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

UPDATE dimension_snapshots
SET source_manifest = JSON_ARRAY()
WHERE source_manifest IS NULL;

ALTER TABLE dimension_snapshots
  MODIFY COLUMN source_manifest JSON NOT NULL;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.table_constraints WHERE table_schema = @schema_name AND table_name = 'dimension_snapshots' AND constraint_name = 'chk_dimension_snapshot_source_manifest') = 0,
  'ALTER TABLE dimension_snapshots
     ADD CONSTRAINT chk_dimension_snapshot_source_manifest
     CHECK (source_manifest_version IN (0,1) AND JSON_TYPE(source_manifest) = ''ARRAY'' AND (source_manifest_version <> 0 OR JSON_LENGTH(source_manifest) = 0))',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
