-- A Flow retention policy may now authorize automatic raw-day deletion: once a
-- day's deletion evidence is complete, the server approves it, publishes the
-- delete barrier and schedules the job as the system actor, instead of waiting
-- for a manual approve/execute. It may also waive the Kafka offset-coverage gate
-- for installations that have not deployed Flow reconciliation. Existing
-- revisions keep the manual, coverage-required semantics.
ALTER TABLE flow_retention_policy_revisions
  ADD COLUMN auto_delete TINYINT(1) NOT NULL DEFAULT 0 AFTER require_backup_before_delete,
  ADD COLUMN waive_kafka_coverage TINYINT(1) NOT NULL DEFAULT 0 AFTER auto_delete,
  ADD CONSTRAINT chk_flow_retention_auto_delete CHECK (auto_delete = 0 OR raw_delete_enabled = 1);
