-- PLAT-04A2d: keep object-GC discovery tenant-bounded and enforce that an
-- object deletion marker can only exist after an explicit retired retention
-- horizon. Actual safety is revalidated transactionally against activations,
-- fact references and latest installed consumer acknowledgements.
SET @schema_name = DATABASE();

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = @schema_name AND table_name = 'dimension_snapshots' AND index_name = 'idx_dimension_snapshot_gc') = 0,
  'ALTER TABLE dimension_snapshots
     ADD KEY idx_dimension_snapshot_gc
       (tenant_id, module_key, dimension_key, status, object_deleted_at, retention_until, id)',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.table_constraints WHERE constraint_schema = @schema_name AND table_name = 'dimension_snapshots' AND constraint_name = 'chk_dimension_snapshot_object_gc') = 0,
  'ALTER TABLE dimension_snapshots
     ADD CONSTRAINT chk_dimension_snapshot_object_gc CHECK (
       object_deleted_at IS NULL OR (
         status = ''retired'' AND retention_until IS NOT NULL
         AND object_deleted_at >= retention_until
       )
     )',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
