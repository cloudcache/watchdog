-- PLAT-04A/04C: close the immutable dimension publication lifecycle without
-- changing migration 040. Existing snapshots are trusted legacy publications;
-- new snapshots default to pending approval. Activation is an append-only,
-- event-time timeline so rollback never rewrites an old snapshot or fact.
SET @schema_name = DATABASE();

SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = 'dimension_snapshots' AND column_name = 'approval_state') = 0,
  'ALTER TABLE dimension_snapshots
     ADD COLUMN approval_state VARCHAR(16) NOT NULL DEFAULT ''approved'' AFTER status,
     ADD COLUMN decided_by CHAR(26) NULL AFTER approval_state,
     ADD COLUMN decided_at DATETIME(3) NULL AFTER decided_by,
     ADD COLUMN decision_reason VARCHAR(512) NULL AFTER decided_at,
     ADD COLUMN signature_algorithm VARCHAR(32) NULL AFTER decision_reason,
     ADD COLUMN signing_key_id VARCHAR(128) NULL AFTER signature_algorithm,
     ADD COLUMN signature VARBINARY(512) NULL AFTER signing_key_id,
     ADD COLUMN signed_at DATETIME(3) NULL AFTER signature,
     ADD COLUMN retention_until DATETIME(3) NULL AFTER signed_at,
     ADD COLUMN object_deleted_at DATETIME(3) NULL AFTER retention_until,
     ADD KEY idx_dimension_snapshot_approval (tenant_id, module_key, dimension_key, approval_state, version),
     ADD KEY idx_dimension_snapshot_retention (status, retention_until, object_deleted_at),
     ADD CONSTRAINT fk_dimension_snapshot_decider FOREIGN KEY (decided_by) REFERENCES users(id) ON DELETE SET NULL,
     ADD CONSTRAINT dimension_snapshots_chk_approval CHECK (approval_state IN (''pending'',''approved'',''rejected'')),
     ADD CONSTRAINT dimension_snapshots_chk_signature CHECK ((signature IS NULL AND signature_algorithm IS NULL AND signing_key_id IS NULL AND signed_at IS NULL) OR (signature IS NOT NULL AND signature_algorithm IS NOT NULL AND signing_key_id IS NOT NULL AND signed_at IS NOT NULL))',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Keep migrated rows approved, but make every subsequently created snapshot
-- enter the explicit approval path unless its INSERT says otherwise.
ALTER TABLE dimension_snapshots
  ALTER COLUMN approval_state SET DEFAULT 'pending';

CREATE TABLE IF NOT EXISTS dimension_snapshot_activations (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  module_key VARCHAR(64) NOT NULL,
  dimension_key VARCHAR(64) NOT NULL,
  snapshot_id CHAR(26) NOT NULL,
  effective_from DATETIME(3) NOT NULL,
  reason VARCHAR(16) NOT NULL,
  rollback_of_snapshot_id CHAR(26) NULL,
  created_by CHAR(26) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_dimension_activation_tenant_id (tenant_id, id),
  UNIQUE KEY uq_dimension_activation_effective (tenant_id, module_key, dimension_key, effective_from),
  KEY idx_dimension_activation_snapshot (tenant_id, snapshot_id, effective_from),
  KEY idx_dimension_activation_rollback (tenant_id, rollback_of_snapshot_id),
  CONSTRAINT fk_dimension_activation_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT fk_dimension_activation_snapshot FOREIGN KEY (tenant_id, snapshot_id)
    REFERENCES dimension_snapshots(tenant_id, id) ON DELETE RESTRICT,
  CONSTRAINT fk_dimension_activation_rollback FOREIGN KEY (tenant_id, rollback_of_snapshot_id)
    REFERENCES dimension_snapshots(tenant_id, id) ON DELETE RESTRICT,
  CONSTRAINT fk_dimension_activation_creator FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL,
  CHECK (reason IN ('publish','rollback')),
  CHECK ((reason = 'publish' AND rollback_of_snapshot_id IS NULL) OR (reason = 'rollback' AND rollback_of_snapshot_id IS NOT NULL))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Every pre-042 snapshot already had event-time activation semantics. Reuse
-- its stable id for a deterministic, idempotent backfill activation id.
INSERT IGNORE INTO dimension_snapshot_activations (
  id, tenant_id, module_key, dimension_key, snapshot_id, effective_from,
  reason, rollback_of_snapshot_id, created_by, created_at
)
SELECT id, tenant_id, module_key, dimension_key, id, effective_from,
       'publish', NULL, created_by, created_at
FROM dimension_snapshots;

-- Consumers report an aggregate event-time envelope per immutable snapshot.
-- GC may remove an object only after every reference's retain_until and the
-- snapshot retention_until have passed; base facts themselves stay in CH.
CREATE TABLE IF NOT EXISTS dimension_snapshot_references (
  tenant_id CHAR(26) NOT NULL,
  snapshot_id CHAR(26) NOT NULL,
  consumer_kind VARCHAR(32) NOT NULL,
  consumer_id VARCHAR(190) NOT NULL,
  min_event_time DATETIME(3) NOT NULL,
  max_event_time DATETIME(3) NOT NULL,
  retain_until DATETIME(3) NOT NULL,
  last_observed_at DATETIME(3) NOT NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  PRIMARY KEY (tenant_id, snapshot_id, consumer_kind, consumer_id),
  KEY idx_dimension_reference_retention (tenant_id, retain_until),
  CONSTRAINT fk_dimension_reference_snapshot FOREIGN KEY (tenant_id, snapshot_id)
    REFERENCES dimension_snapshots(tenant_id, id) ON DELETE RESTRICT,
  CONSTRAINT fk_dimension_reference_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CHECK (min_event_time <= max_event_time),
  CHECK (max_event_time <= retain_until)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Acks now carry download/install failure state. installed_at becomes nullable
-- so a worker can report a failed attempt before any object was installed.
SET @sql = IF(
  (SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = @schema_name AND table_name = 'dimension_snapshot_acks' AND column_name = 'state') = 0,
  'ALTER TABLE dimension_snapshot_acks
     ADD COLUMN state VARCHAR(16) NOT NULL DEFAULT ''installed'' AFTER checksum,
     ADD COLUMN attempted_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) AFTER state,
     ADD COLUMN error_code VARCHAR(64) NULL AFTER installed_at,
     ADD COLUMN error_message VARCHAR(512) NULL AFTER error_code,
     ADD COLUMN row_version BIGINT UNSIGNED NOT NULL DEFAULT 1 AFTER error_message,
     MODIFY COLUMN installed_at DATETIME(3) NULL,
     ADD KEY idx_dimension_snapshot_acks_state (tenant_id, snapshot_id, state, attempted_at),
     ADD CONSTRAINT dimension_snapshot_acks_chk_state CHECK (state IN (''downloaded'',''installed'',''failed'')),
     ADD CONSTRAINT dimension_snapshot_acks_chk_installed CHECK (state <> ''installed'' OR installed_at IS NOT NULL)',
  'SELECT 1'
);
PREPARE stmt FROM @sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;
