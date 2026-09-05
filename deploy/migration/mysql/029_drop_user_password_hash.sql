-- PLAT P0: identity is an authorization projection of the external IdP
-- (PocketBase), which is the sole authentication authority. The password_hash
-- column was retained nullable by migration 011 only for the rollback window;
-- no code path reads or writes it (CreateUser stopped referencing it in the
-- single-authority slice). Drop it so MySQL holds no credential material at
-- all — the dual credential store is now fully eliminated.
--
-- Guarded so re-running the migration is a no-op (idempotent), matching the
-- conditional-DDL pattern used across this migration set.
SET @schema_name = DATABASE();

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = 'users' AND column_name = 'password_hash') > 0,
  'ALTER TABLE users DROP COLUMN password_hash',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
