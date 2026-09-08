-- KISS-04A: complete the single-domain agent registry lifecycle. Agent plan
-- delivery remains a separate slice because every agent kind must validate and
-- apply its payload before an "installed" acknowledgement is truthful.

ALTER TABLE agents
  MODIFY COLUMN status VARCHAR(16) NOT NULL DEFAULT 'registered',
  ADD COLUMN heartbeat_interval_seconds INT UNSIGNED NOT NULL DEFAULT 60 AFTER capabilities_json,
  ADD COLUMN clock_skew_seconds INT NOT NULL DEFAULT 0 AFTER heartbeat_interval_seconds;

UPDATE agents SET status = CASE status
  WHEN 'pending' THEN 'registered'
  WHEN 'up' THEN 'active'
  WHEN 'registered' THEN 'registered'
  WHEN 'active' THEN 'active'
  WHEN 'draining' THEN 'draining'
  WHEN 'revoked' THEN 'revoked'
  WHEN 'disabled' THEN 'revoked'
  ELSE 'active'
END;

ALTER TABLE agent_credentials
  ADD UNIQUE KEY uq_agent_credentials_mtls (mtls_fingerprint);

ALTER TABLE agent_enrollment_tokens
  ADD COLUMN device_id CHAR(26) NULL AFTER allowed_kind,
  ADD KEY idx_agent_enrollment_device (device_id),
  ADD CONSTRAINT fk_agent_enrollment_device FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE SET NULL;
