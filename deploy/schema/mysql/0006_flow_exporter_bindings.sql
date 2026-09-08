-- Flow exporter configuration is a capability of an existing device, not a
-- second device inventory. One device can have multiple protocol/source/domain
-- bindings, all of which retain the stable devices.id used by ClickHouse facts.

CREATE TABLE IF NOT EXISTS flow_exporter_bindings (
  id                       CHAR(26)     NOT NULL,
  device_id                CHAR(26)     NOT NULL,
  collector_agent_id       CHAR(26)     NULL,
  source_prefix            VARCHAR(48)  CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  protocol                 VARCHAR(16)  CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  observation_domain_id    BIGINT UNSIGNED NULL,
  observation_domain_key   VARCHAR(20)  CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT '*',
  sampling_mode            VARCHAR(16)  CHARACTER SET ascii COLLATE ascii_bin NOT NULL DEFAULT 'sampled',
  default_sampling_rate    BIGINT UNSIGNED NOT NULL DEFAULT 0,
  sampling_rules_json      JSON         NOT NULL,
  observations_json        JSON         NOT NULL,
  enabled                  TINYINT(1)   NOT NULL DEFAULT 1,
  ownership_epoch          BIGINT UNSIGNED NOT NULL DEFAULT 1,
  published_row_version    BIGINT UNSIGNED NOT NULL DEFAULT 0,
  published_plan_version   BIGINT UNSIGNED NOT NULL DEFAULT 0,
  row_version              BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_by               CHAR(26)     NULL,
  updated_by               CHAR(26)     NULL,
  created_at               DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at               DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_flow_exporter_selector (protocol, source_prefix, observation_domain_key),
  KEY idx_flow_exporter_device (device_id, enabled),
  KEY idx_flow_exporter_collector (collector_agent_id, enabled),
  CONSTRAINT fk_flow_exporter_device FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE RESTRICT,
  CONSTRAINT fk_flow_exporter_collector FOREIGN KEY (collector_agent_id) REFERENCES agents(id) ON DELETE RESTRICT,
  CHECK (protocol IN ('sflow5','netflow5','netflow9','ipfix')),
  CHECK (sampling_mode IN ('sampled','pre_scaled')),
  CHECK (ownership_epoch > 0),
  CHECK ((observation_domain_id IS NULL AND observation_domain_key = '*') OR
         (observation_domain_id IS NOT NULL AND observation_domain_key <> '*')),
  CHECK (published_row_version <= row_version),
  CHECK ((published_row_version = 0) = (published_plan_version = 0))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
