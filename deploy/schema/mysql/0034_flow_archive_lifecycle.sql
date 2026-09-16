-- Rotate bounded late-arrival conservation checks across reconciled raw days.
-- This is scheduler evidence only; it does not enable physical deletion.
ALTER TABLE flow_retention_partition_states
  ADD COLUMN late_checked_at DATETIME(3) NULL AFTER reconciled_at,
  ADD KEY idx_flow_retention_late_check (state, late_checked_at, source_date);
