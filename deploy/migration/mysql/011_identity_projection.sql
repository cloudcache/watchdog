-- Expand users into a PocketBase-authenticated identity projection.
-- Existing password hashes remain temporarily readable for rollback, but the
-- platform runtime never verifies them after this migration.

SET @schema_name := DATABASE();

SET @sql := IF(
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = 'users' AND column_name = 'auth_provider') = 0,
  'ALTER TABLE users ADD COLUMN auth_provider VARCHAR(32) NULL AFTER status',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql := IF(
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = 'users' AND column_name = 'external_subject_id') = 0,
  'ALTER TABLE users ADD COLUMN external_subject_id VARCHAR(64) NULL AFTER auth_provider',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

ALTER TABLE users MODIFY COLUMN password_hash VARCHAR(255) NULL;

SET @sql := IF(
  (SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = @schema_name AND table_name = 'users' AND index_name = 'uq_users_external_identity') = 0,
  'ALTER TABLE users ADD UNIQUE KEY uq_users_external_identity (auth_provider, external_subject_id, tenant_id)',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql := IF(
  (SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = @schema_name AND table_name = 'users' AND index_name = 'idx_users_external_identity') = 0,
  'ALTER TABLE users ADD KEY idx_users_external_identity (auth_provider, external_subject_id)',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
