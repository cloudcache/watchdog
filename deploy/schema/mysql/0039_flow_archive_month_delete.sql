-- One active destructive approval per storage partition. Revocation clears the
-- generated key, allowing a later approval after evidence is rebuilt without
-- permitting two workers to race the same raw day or archive month.
ALTER TABLE flow_deletion_approvals
  ADD COLUMN active_partition_start DATE
    GENERATED ALWAYS AS (IF(status = 'approved', partition_start, NULL)) STORED AFTER status,
  ADD UNIQUE KEY uq_flow_deletion_active_partition (storage_kind, active_partition_start);
