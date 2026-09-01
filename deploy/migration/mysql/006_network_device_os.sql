SET @schema_name := DATABASE();

SET @exists := (
  SELECT COUNT(*)
  FROM information_schema.columns
  WHERE table_schema = @schema_name
    AND table_name = 'network_devices'
    AND column_name = 'platform'
);
SET @sql := IF(
  @exists = 0,
  'ALTER TABLE network_devices ADD COLUMN platform VARCHAR(190) NOT NULL DEFAULT '''' AFTER model',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @exists := (
  SELECT COUNT(*)
  FROM information_schema.columns
  WHERE table_schema = @schema_name
    AND table_name = 'network_devices'
    AND column_name = 'os_name'
);
SET @sql := IF(
  @exists = 0,
  'ALTER TABLE network_devices ADD COLUMN os_name VARCHAR(128) NOT NULL DEFAULT '''' AFTER platform',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @exists := (
  SELECT COUNT(*)
  FROM information_schema.columns
  WHERE table_schema = @schema_name
    AND table_name = 'network_devices'
    AND column_name = 'os_version'
);
SET @sql := IF(
  @exists = 0,
  'ALTER TABLE network_devices ADD COLUMN os_version VARCHAR(128) NOT NULL DEFAULT '''' AFTER os_name',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
