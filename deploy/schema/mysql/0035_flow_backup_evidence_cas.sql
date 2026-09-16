-- Backup evidence is managed independently from retention policy revisions.
-- restore_test_ref makes the restore exercise traceable; row_version provides
-- compare-and-swap revocation so stale operators cannot change evidence state.
ALTER TABLE flow_backup_restore_evidence
  ADD COLUMN restore_test_ref VARCHAR(512) NOT NULL DEFAULT '' AFTER restore_tested_at,
  ADD COLUMN row_version BIGINT UNSIGNED NOT NULL DEFAULT 1 AFTER revoked_at;
