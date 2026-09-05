-- Persist low-frequency collector runtime observations separately from desired
-- configuration/version state. Heartbeats never advance row_version, plan ACK
-- or last-known-good versions.

ALTER TABLE collector_agents
  ADD COLUMN runtime_schema_version SMALLINT UNSIGNED NOT NULL DEFAULT 0
    AFTER boot_id,
  ADD COLUMN heartbeat_sequence BIGINT UNSIGNED NOT NULL DEFAULT 0
    AFTER runtime_schema_version,
  ADD COLUMN heartbeat_sent_at DATETIME(3) NULL
    AFTER heartbeat_sequence,
  ADD COLUMN clock_offset_ms BIGINT NULL
    AFTER heartbeat_sent_at,
  ADD COLUMN runtime_observation_json JSON NULL
    AFTER clock_offset_ms,
  ADD COLUMN runtime_observation_hash CHAR(64) NOT NULL DEFAULT ''
    AFTER runtime_observation_json,
  ADD COLUMN heartbeat_payload_hash CHAR(64) NOT NULL DEFAULT ''
    AFTER runtime_observation_hash,
  ADD CONSTRAINT chk_collector_runtime_payload
    CHECK (
      (runtime_schema_version = 0
        AND heartbeat_sequence = 0
        AND heartbeat_sent_at IS NULL
        AND clock_offset_ms IS NULL
        AND runtime_observation_json IS NULL
        AND runtime_observation_hash = ''
        AND heartbeat_payload_hash = '')
      OR
      (runtime_schema_version > 0
        AND heartbeat_sequence > 0
        AND heartbeat_sent_at IS NOT NULL
        AND clock_offset_ms IS NOT NULL
        AND capabilities_json IS NOT NULL
        AND CHAR_LENGTH(capabilities_hash) = 64
        AND runtime_observation_json IS NOT NULL
        AND CHAR_LENGTH(runtime_observation_hash) = 64
        AND CHAR_LENGTH(heartbeat_payload_hash) = 64)
    );
