-- Device inventory child tables previously only defined in install/init.sql.
-- Adds them to the numbered migration path so pure-migration deployments are
-- not missing device_physical_entities / device_vlans / device_lag_groups.

CREATE TABLE IF NOT EXISTS device_physical_entities (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  device_id CHAR(26) NOT NULL,
  entity_index INT UNSIGNED NOT NULL,
  name VARCHAR(255) NOT NULL DEFAULT '',
  description VARCHAR(512) NOT NULL DEFAULT '',
  class VARCHAR(64) NOT NULL DEFAULT '',
  vendor_type VARCHAR(255) NOT NULL DEFAULT '',
  contained_in INT UNSIGNED NOT NULL DEFAULT 0,
  hardware_revision VARCHAR(64) NOT NULL DEFAULT '',
  serial_number VARCHAR(128) NOT NULL DEFAULT '',
  manufacturer_name VARCHAR(128) NOT NULL DEFAULT '',
  model_name VARCHAR(128) NOT NULL DEFAULT '',
  is_fru BOOLEAN NOT NULL DEFAULT FALSE,
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_device_entity (tenant_id, device_id, entity_index),
  KEY idx_device_entities_device (tenant_id, device_id),
  CONSTRAINT fk_device_entities_device FOREIGN KEY (device_id) REFERENCES network_devices(id) ON DELETE CASCADE,
  CONSTRAINT fk_device_entities_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS device_vlans (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  device_id CHAR(26) NOT NULL,
  vlan_id INT UNSIGNED NOT NULL,
  name VARCHAR(190) NOT NULL DEFAULT '',
  status VARCHAR(32) NOT NULL DEFAULT 'active',
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_device_vlan (tenant_id, device_id, vlan_id),
  KEY idx_device_vlans_device (tenant_id, device_id),
  CONSTRAINT fk_device_vlans_device FOREIGN KEY (device_id) REFERENCES network_devices(id) ON DELETE CASCADE,
  CONSTRAINT fk_device_vlans_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS device_lag_groups (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  device_id CHAR(26) NOT NULL,
  aggregate_index INT UNSIGNED NOT NULL,
  mac_address VARCHAR(32) NOT NULL DEFAULT '',
  mode VARCHAR(32) NOT NULL DEFAULT '',
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_device_lag (tenant_id, device_id, aggregate_index),
  KEY idx_device_lag_device (tenant_id, device_id),
  CONSTRAINT fk_device_lag_device FOREIGN KEY (device_id) REFERENCES network_devices(id) ON DELETE CASCADE,
  CONSTRAINT fk_device_lag_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
