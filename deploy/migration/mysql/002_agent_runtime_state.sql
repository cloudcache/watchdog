SET @schema_name = DATABASE();

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = 'target_agents' AND column_name = 'last_run_at') = 0,
  'ALTER TABLE target_agents ADD COLUMN last_run_at DATETIME(3) NULL AFTER last_seen_at',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = 'target_agents' AND column_name = 'last_success_at') = 0,
  'ALTER TABLE target_agents ADD COLUMN last_success_at DATETIME(3) NULL AFTER last_run_at',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = 'target_agents' AND column_name = 'last_error') = 0,
  'ALTER TABLE target_agents ADD COLUMN last_error TEXT NULL AFTER last_success_at',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = 'target_agents' AND column_name = 'run_count') = 0,
  'ALTER TABLE target_agents ADD COLUMN run_count BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER last_error',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = 'target_agents' AND column_name = 'failure_count') = 0,
  'ALTER TABLE target_agents ADD COLUMN failure_count BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER run_count',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = @schema_name AND table_name = 'target_agents' AND index_name = 'idx_target_agents_run') = 0,
  'ALTER TABLE target_agents ADD KEY idx_target_agents_run (agent_type, mode, status, last_run_at, failure_count)',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
