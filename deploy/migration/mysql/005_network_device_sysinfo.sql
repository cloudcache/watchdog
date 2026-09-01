SET @has_sys_location := (
  SELECT COUNT(*)
  FROM information_schema.columns
  WHERE table_schema = DATABASE()
    AND table_name = 'network_devices'
    AND column_name = 'sys_location'
);
SET @ddl := IF(
  @has_sys_location = 0,
  'ALTER TABLE network_devices ADD COLUMN sys_location VARCHAR(255) NOT NULL DEFAULT '''' AFTER sys_descr',
  'SELECT 1'
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @has_uptime_seconds := (
  SELECT COUNT(*)
  FROM information_schema.columns
  WHERE table_schema = DATABASE()
    AND table_name = 'network_devices'
    AND column_name = 'uptime_seconds'
);
SET @ddl := IF(
  @has_uptime_seconds = 0,
  'ALTER TABLE network_devices ADD COLUMN uptime_seconds BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER sys_location',
  'SELECT 1'
);
PREPARE stmt FROM @ddl;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
