CREATE TABLE IF NOT EXISTS snmp_os_definitions (
  id CHAR(26) PRIMARY KEY,
  os_name VARCHAR(128) NOT NULL,
  os_group VARCHAR(128) NOT NULL DEFAULT '',
  vendor VARCHAR(128) NOT NULL DEFAULT '',
  class VARCHAR(64) NOT NULL DEFAULT '',
  definition_json JSON NOT NULL,
  source VARCHAR(64) NOT NULL DEFAULT 'librenms',
  source_version VARCHAR(128) NOT NULL DEFAULT '',
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_snmp_os_definitions_name_source (os_name, source),
  KEY idx_snmp_os_definitions_group (os_group),
  KEY idx_snmp_os_definitions_vendor (vendor)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS snmp_module_definitions (
  id CHAR(26) PRIMARY KEY,
  module_name VARCHAR(96) NOT NULL,
  module_type VARCHAR(32) NOT NULL,
  definition_json JSON NOT NULL,
  source VARCHAR(64) NOT NULL DEFAULT 'librenms',
  source_version VARCHAR(128) NOT NULL DEFAULT '',
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_snmp_module_definitions_name_type_source (module_name, module_type, source),
  KEY idx_snmp_module_definitions_type (module_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS snmp_device_modules (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  device_id CHAR(26) NOT NULL,
  module_name VARCHAR(96) NOT NULL,
  discovery_enabled TINYINT(1) NOT NULL DEFAULT 1,
  polling_enabled TINYINT(1) NOT NULL DEFAULT 1,
  discovery_status VARCHAR(32) NOT NULL DEFAULT 'unknown',
  polling_status VARCHAR(32) NOT NULL DEFAULT 'unknown',
  last_discovered_at DATETIME(3) NULL,
  last_polled_at DATETIME(3) NULL,
  last_error TEXT NULL,
  metadata_json JSON NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_snmp_device_modules (tenant_id, device_id, module_name),
  KEY idx_snmp_device_modules_status (tenant_id, device_id, polling_enabled),
  CONSTRAINT fk_snmp_device_modules_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT fk_snmp_device_modules_device FOREIGN KEY (device_id) REFERENCES network_devices(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS snmp_state_translations (
  id CHAR(26) PRIMARY KEY,
  name VARCHAR(190) NOT NULL,
  source VARCHAR(64) NOT NULL DEFAULT 'librenms',
  states_json JSON NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_snmp_state_translations_name_source (name, source)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS snmp_collection_recipes (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  device_id CHAR(26) NOT NULL,
  entity_type VARCHAR(48) NOT NULL,
  entity_id CHAR(26) NOT NULL DEFAULT '',
  module_name VARCHAR(64) NOT NULL,
  metric_name VARCHAR(160) NOT NULL,
  value_type VARCHAR(32) NOT NULL,
  oid VARCHAR(512) NOT NULL,
  numeric_oid VARCHAR(512) NOT NULL,
  oid_index VARCHAR(96) NOT NULL DEFAULT '',
  mib VARCHAR(128) NOT NULL DEFAULT '',
  context_name VARCHAR(96) NOT NULL DEFAULT '',
  poller_type VARCHAR(48) NOT NULL DEFAULT 'snmp',
  divisor DOUBLE NULL,
  multiplier DOUBLE NULL,
  user_func VARCHAR(128) NOT NULL DEFAULT '',
  state_map_id CHAR(26) NULL,
  unit VARCHAR(64) NOT NULL DEFAULT '',
  sample_interval_seconds INT UNSIGNED NOT NULL DEFAULT 60,
  labels_json JSON NULL,
  options_json JSON NULL,
  enabled TINYINT(1) NOT NULL DEFAULT 1,
  discovered_at DATETIME(3) NOT NULL,
  last_seen_at DATETIME(3) NOT NULL,
  last_polled_at DATETIME(3) NULL,
  last_error TEXT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_snmp_collection_recipe (
    tenant_id, device_id, module_name, entity_type, entity_id, metric_name, oid_index, context_name
  ),
  KEY idx_snmp_collection_due (tenant_id, enabled, sample_interval_seconds, last_polled_at),
  KEY idx_snmp_collection_device_module (tenant_id, device_id, module_name),
  KEY idx_snmp_collection_metric (tenant_id, metric_name),
  CONSTRAINT fk_snmp_collection_recipes_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT fk_snmp_collection_recipes_device FOREIGN KEY (device_id) REFERENCES network_devices(id) ON DELETE CASCADE,
  CONSTRAINT fk_snmp_collection_recipes_state_map FOREIGN KEY (state_map_id) REFERENCES snmp_state_translations(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS snmp_trap_handlers (
  id CHAR(26) PRIMARY KEY,
  trap_oid VARCHAR(512) NOT NULL,
  handler_key VARCHAR(190) NOT NULL,
  enabled TINYINT(1) NOT NULL DEFAULT 1,
  options_json JSON NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_snmp_trap_handlers_oid (trap_oid),
  KEY idx_snmp_trap_handlers_enabled (enabled, handler_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS snmp_events (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  device_id CHAR(26) NOT NULL,
  entity_type VARCHAR(48) NOT NULL DEFAULT '',
  entity_id CHAR(26) NOT NULL DEFAULT '',
  source VARCHAR(32) NOT NULL,
  severity VARCHAR(32) NOT NULL DEFAULT 'info',
  event_type VARCHAR(96) NOT NULL,
  message TEXT NOT NULL,
  raw_json JSON NULL,
  occurred_at DATETIME(3) NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  KEY idx_snmp_events_device_time (tenant_id, device_id, occurred_at),
  KEY idx_snmp_events_entity_time (tenant_id, entity_type, entity_id, occurred_at),
  CONSTRAINT fk_snmp_events_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT fk_snmp_events_device FOREIGN KEY (device_id) REFERENCES network_devices(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
