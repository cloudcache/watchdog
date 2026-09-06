-- PLAT-04A2c: bound observed-consumer readiness and version-drift reads.
-- No second consumer registry is introduced; the API reports only workers
-- that have written at least one immutable snapshot acknowledgement.
SET @schema_name = DATABASE();

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = @schema_name AND table_name = 'dimension_snapshot_acks' AND index_name = 'idx_dimension_snapshot_acks_observed') = 0,
  'ALTER TABLE dimension_snapshot_acks
     ADD KEY idx_dimension_snapshot_acks_observed
       (tenant_id, worker_id, attempted_at, snapshot_id)',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
