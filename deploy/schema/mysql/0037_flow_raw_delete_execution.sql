-- Raw UTC-day deletion execution keeps one immutable receipt per approval
-- attempt. A revoked/failed approval may be replaced without pretending the
-- earlier attempt never happened, while one approval can still enqueue only
-- one destructive operation.
ALTER TABLE flow_deletion_receipts
  DROP FOREIGN KEY fk_flow_deletion_receipt_approval;

ALTER TABLE flow_deletion_receipts
  DROP INDEX uq_flow_deletion_partition,
  MODIFY COLUMN deletion_approval_id CHAR(26) NOT NULL,
  ADD COLUMN source_physical_record_count BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER source_record_count,
  ADD COLUMN source_estimated_valid_records BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER source_estimated_packets,
  ADD COLUMN post_delete_record_count BIGINT UNSIGNED NULL AFTER ch_query_id,
  ADD COLUMN row_version BIGINT UNSIGNED NOT NULL DEFAULT 1 AFTER error_detail,
  ADD UNIQUE KEY uq_flow_deletion_receipt_approval (deletion_approval_id),
  ADD CONSTRAINT fk_flow_deletion_receipt_approval FOREIGN KEY (deletion_approval_id) REFERENCES flow_deletion_approvals(id) ON DELETE RESTRICT;

ALTER TABLE flow_deletion_approvals
  ADD COLUMN source_physical_record_count BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER source_record_count;
