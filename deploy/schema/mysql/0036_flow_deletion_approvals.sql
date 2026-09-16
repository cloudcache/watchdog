-- Approval freezes the exact evidence reviewed before a destructive job may
-- be enqueued. It is global, immutable apart from explicit revocation, and
-- deliberately separate from the eventual ClickHouse deletion receipt.
CREATE TABLE IF NOT EXISTS flow_deletion_approvals (
  id                                CHAR(26)        NOT NULL,
  storage_kind                      VARCHAR(16)     NOT NULL,
  partition_granularity             VARCHAR(8)      NOT NULL,
  partition_start                   DATE            NOT NULL,
  partition_end                     DATE            NOT NULL,
  policy_id                         CHAR(26)        NOT NULL,
  policy_version                    BIGINT UNSIGNED NOT NULL,
  generation                        BIGINT UNSIGNED NOT NULL,
  backup_evidence_id                CHAR(26)        NULL,
  kafka_coverage_json               JSON            NOT NULL,
  source_record_count               BIGINT UNSIGNED NOT NULL,
  source_raw_bytes                  BIGINT UNSIGNED NOT NULL,
  source_raw_packets                BIGINT UNSIGNED NOT NULL,
  source_estimated_bytes            BIGINT UNSIGNED NOT NULL,
  source_estimated_packets          BIGINT UNSIGNED NOT NULL,
  source_estimated_valid_records    BIGINT UNSIGNED NOT NULL,
  archive_record_count              BIGINT UNSIGNED NOT NULL,
  archive_raw_bytes                 BIGINT UNSIGNED NOT NULL,
  archive_raw_packets               BIGINT UNSIGNED NOT NULL,
  archive_estimated_bytes           BIGINT UNSIGNED NOT NULL,
  archive_estimated_packets         BIGINT UNSIGNED NOT NULL,
  archive_estimated_valid_records   BIGINT UNSIGNED NOT NULL,
  status                            VARCHAR(16)     NOT NULL DEFAULT 'approved',
  approved_by                       CHAR(26)        NOT NULL,
  approved_at                       DATETIME(3)     NOT NULL,
  revoked_by                        CHAR(26)        NULL,
  revoked_at                        DATETIME(3)     NULL,
  row_version                       BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_at                        DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  KEY idx_flow_deletion_approval_partition (storage_kind, partition_start, generation, status),
  KEY idx_flow_deletion_approval_status (status, approved_at),
  CONSTRAINT fk_flow_deletion_approval_policy FOREIGN KEY (policy_id) REFERENCES flow_retention_policy_revisions(id) ON DELETE RESTRICT,
  CONSTRAINT fk_flow_deletion_approval_backup FOREIGN KEY (backup_evidence_id) REFERENCES flow_backup_restore_evidence(id) ON DELETE RESTRICT,
  CONSTRAINT fk_flow_deletion_approval_actor FOREIGN KEY (approved_by) REFERENCES users(id) ON DELETE RESTRICT,
  CONSTRAINT fk_flow_deletion_revocation_actor FOREIGN KEY (revoked_by) REFERENCES users(id) ON DELETE RESTRICT,
  CHECK (storage_kind IN ('raw', 'archive')),
  CHECK (partition_granularity IN ('day', 'month')),
  CHECK ((storage_kind = 'raw' AND partition_granularity = 'day' AND partition_end = partition_start + INTERVAL 1 DAY)
      OR (storage_kind = 'archive' AND partition_granularity = 'month' AND DAY(partition_start) = 1 AND partition_end = partition_start + INTERVAL 1 MONTH)),
  CHECK (status IN ('approved', 'revoked')),
  CHECK ((status = 'approved' AND revoked_at IS NULL AND revoked_by IS NULL)
      OR (status = 'revoked' AND revoked_at IS NOT NULL AND revoked_by IS NOT NULL))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

ALTER TABLE flow_retention_partition_states
  ADD COLUMN delete_approval_id CHAR(26) NULL AFTER delete_job_id,
  ADD CONSTRAINT fk_flow_retention_delete_approval FOREIGN KEY (delete_approval_id) REFERENCES flow_deletion_approvals(id) ON DELETE RESTRICT;

ALTER TABLE flow_deletion_receipts
  ADD COLUMN deletion_approval_id CHAR(26) NULL AFTER operation_job_id,
  ADD CONSTRAINT fk_flow_deletion_receipt_approval FOREIGN KEY (deletion_approval_id) REFERENCES flow_deletion_approvals(id) ON DELETE RESTRICT;
