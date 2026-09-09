-- KISS-04B: immutable, signed, single-domain agent plan delivery and ACK.
-- No tenant/fleet/canary state is introduced; bulk fan-out uses operation_jobs.

ALTER TABLE agent_plans
  ADD COLUMN kind VARCHAR(32) NOT NULL AFTER agent_id,
  ADD COLUMN source_job_id CHAR(26) NULL AFTER kind,
  ADD COLUMN api_version VARCHAR(32) NOT NULL DEFAULT 'v1' AFTER kind,
  ADD COLUMN required_capabilities_json JSON NOT NULL AFTER api_version,
  ADD COLUMN payload_sha256 CHAR(64) NOT NULL AFTER payload_json,
  ADD COLUMN signing_key_id VARCHAR(64) NOT NULL AFTER payload_sha256,
  MODIFY COLUMN signature VARBINARY(64) NOT NULL,
  ADD COLUMN not_before DATETIME(3) NULL AFTER signature,
  ADD COLUMN expires_at DATETIME(3) NOT NULL AFTER not_before,
  ADD COLUMN supersedes_plan_version BIGINT UNSIGNED NOT NULL DEFAULT 0 AFTER expires_at,
  ADD KEY idx_agent_plans_created (agent_id, created_at),
  ADD UNIQUE KEY uq_agent_plan_rollout (source_job_id, agent_id),
  ADD CONSTRAINT chk_agent_plan_lineage CHECK (supersedes_plan_version < plan_version);

ALTER TABLE agent_plan_acks
  DROP INDEX uq_agent_plan_ack,
  ADD COLUMN payload_sha256 CHAR(64) NOT NULL AFTER plan_version,
  ADD COLUMN boot_id VARCHAR(64) NOT NULL AFTER status,
  ADD COLUMN software_version VARCHAR(64) NOT NULL DEFAULT '' AFTER boot_id,
  ADD COLUMN error_code VARCHAR(64) NOT NULL DEFAULT '' AFTER software_version,
  ADD UNIQUE KEY uq_agent_plan_ack (agent_id, plan_version, boot_id),
  ADD KEY idx_agent_plan_acks_time (agent_id, acknowledged_at);
