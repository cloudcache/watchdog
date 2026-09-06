-- PLAT-04F system-scope operation jobs: platform-level jobs (Kafka partition
-- maintenance, global retention, cross-tenant receipts) that have no owning
-- tenant. Rather than fabricate a "system tenant" or drop the tenant foreign
-- key, tenant_id becomes nullable and an explicit scope_type marks the two
-- cases. A generated idempotency_domain gives system jobs their own dedup
-- namespace (COALESCE(tenant_id, '__system__')) so the idempotency uniqueness
-- keeps working with a NULL tenant. The lease/retry/heartbeat state machine is
-- untouched: system jobs run through the exact same path as tenant jobs.
SET @schema_name = DATABASE();

-- 1. scope_type: default 'tenant' keeps every existing row valid.
SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = 'operation_jobs' AND column_name = 'scope_type') = 0,
  'ALTER TABLE operation_jobs ADD COLUMN scope_type VARCHAR(16) NOT NULL DEFAULT ''tenant'' AFTER tenant_id',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- 2. Drop the tenant FK before touching the column / rebuilding the table, then
-- re-add it in step 5. Adding a STORED generated column rebuilds the table and
-- otherwise trips MySQL's implicit FK recreation (error 1215).
SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.table_constraints WHERE table_schema = @schema_name AND table_name = 'operation_jobs' AND constraint_name = 'fk_operation_jobs_tenant' AND constraint_type = 'FOREIGN KEY') > 0,
  'ALTER TABLE operation_jobs DROP FOREIGN KEY fk_operation_jobs_tenant',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- 3. tenant_id nullable (NULL for system scope). Re-running MODIFY to the same
-- type is a harmless no-op.
ALTER TABLE operation_jobs MODIFY COLUMN tenant_id CHAR(26) COLLATE utf8mb4_unicode_ci NULL;

-- 4. idempotency_domain: a virtual generated column that folds a NULL tenant to
-- the '__system__' sentinel. No ULID contains an underscore, so a system domain
-- can never collide with a tenant id. It is VIRTUAL (not STORED) because a
-- STORED generated column would make tenant_id its base column, and MySQL then
-- forbids the tenant FK's ON DELETE CASCADE; InnoDB still indexes it uniquely.
SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = 'operation_jobs' AND column_name = 'idempotency_domain') = 0,
  'ALTER TABLE operation_jobs ADD COLUMN idempotency_domain CHAR(26) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci GENERATED ALWAYS AS (COALESCE(tenant_id, ''__system__'')) VIRTUAL AFTER idempotency_key',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- 5. Re-add the tenant FK (unchanged ON DELETE CASCADE); a NULL tenant_id is
-- simply not checked by the constraint.
SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.table_constraints WHERE table_schema = @schema_name AND table_name = 'operation_jobs' AND constraint_name = 'fk_operation_jobs_tenant' AND constraint_type = 'FOREIGN KEY') = 0,
  'ALTER TABLE operation_jobs ADD CONSTRAINT fk_operation_jobs_tenant FOREIGN KEY (tenant_id) REFERENCES tenants (id) ON DELETE CASCADE',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- 6. Swap the idempotency uniqueness from (tenant_id, ...) to
-- (idempotency_domain, ...) so system jobs dedupe among themselves.
SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = @schema_name AND table_name = 'operation_jobs' AND index_name = 'uq_operation_jobs_idempotency') > 0,
  'ALTER TABLE operation_jobs DROP INDEX uq_operation_jobs_idempotency',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.statistics WHERE table_schema = @schema_name AND table_name = 'operation_jobs' AND index_name = 'uq_operation_jobs_idem_domain') = 0,
  'ALTER TABLE operation_jobs ADD UNIQUE KEY uq_operation_jobs_idem_domain (idempotency_domain, job_type, idempotency_key)',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- 7. Correspondence: tenant scope has a tenant, system scope has none.
SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.table_constraints WHERE table_schema = @schema_name AND table_name = 'operation_jobs' AND constraint_name = 'operation_jobs_chk_scope') = 0,
  'ALTER TABLE operation_jobs ADD CONSTRAINT operation_jobs_chk_scope CHECK ((scope_type = ''tenant'' AND tenant_id IS NOT NULL) OR (scope_type = ''system'' AND tenant_id IS NULL))',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
