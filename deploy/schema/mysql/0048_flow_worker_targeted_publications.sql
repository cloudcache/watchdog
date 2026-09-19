-- A Flow device is processed by exactly one worker; one worker may process many
-- devices. Immutable enrichment publications are delivered only to the workers
-- selected by those device bindings. ACK rows remain the installation receipt.

CREATE TABLE IF NOT EXISTS flow_worker_device_bindings (
  device_id    CHAR(26)        NOT NULL,
  worker_id    CHAR(26)        NOT NULL,
  row_version  BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_by   CHAR(26)        NULL,
  updated_by   CHAR(26)        NULL,
  created_at   DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at   DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (device_id),
  KEY idx_flow_worker_device_bindings_worker (worker_id, device_id),
  CONSTRAINT fk_flow_worker_device_binding_device FOREIGN KEY (device_id) REFERENCES devices(id) ON DELETE CASCADE,
  CONSTRAINT fk_flow_worker_device_binding_worker FOREIGN KEY (worker_id) REFERENCES agents(id) ON DELETE RESTRICT,
  CONSTRAINT fk_flow_worker_device_binding_created_by FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL,
  CONSTRAINT fk_flow_worker_device_binding_updated_by FOREIGN KEY (updated_by) REFERENCES users(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS flow_enrichment_publication_targets (
  publication_id CHAR(26)    NOT NULL,
  worker_id      CHAR(26)    NOT NULL,
  created_at     DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (publication_id, worker_id),
  KEY idx_flow_enrichment_publication_targets_worker (worker_id, publication_id),
  CONSTRAINT fk_flow_enrichment_target_publication FOREIGN KEY (publication_id) REFERENCES flow_enrichment_publications(id) ON DELETE CASCADE,
  CONSTRAINT fk_flow_enrichment_target_worker FOREIGN KEY (worker_id) REFERENCES agents(id) ON DELETE RESTRICT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- An existing single-worker installation has no ambiguous placement choice.
-- Preserve that working topology during upgrade; multi-worker installations
-- remain intentionally unbound until an administrator selects ownership.
INSERT IGNORE INTO flow_worker_device_bindings (device_id, worker_id)
SELECT enabled_devices.device_id, only_worker.worker_id
FROM (
  SELECT DISTINCT device_id
  FROM flow_exporter_bindings
  WHERE enabled = 1
) AS enabled_devices
JOIN (
  SELECT MIN(id) AS worker_id
  FROM agents
  WHERE kind = 'flow_worker' AND status = 'active'
  HAVING COUNT(*) = 1
) AS only_worker ON 1 = 1;
