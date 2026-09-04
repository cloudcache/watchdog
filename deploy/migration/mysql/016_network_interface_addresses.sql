CREATE TABLE IF NOT EXISTS network_interface_addresses (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  device_id CHAR(26) NOT NULL,
  port_id CHAR(26) NULL,
  if_index BIGINT UNSIGNED NOT NULL,
  address VARBINARY(16) NOT NULL,
  family VARCHAR(8) NOT NULL,
  prefix_length TINYINT UNSIGNED NOT NULL,
  origin VARCHAR(32) NOT NULL DEFAULT '',
  context_name VARCHAR(96) NOT NULL DEFAULT '',
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_network_interface_addresses (
    tenant_id, device_id, if_index, address, prefix_length, context_name
  ),
  KEY idx_network_interface_addresses_port (tenant_id, port_id, family),
  KEY idx_network_interface_addresses_device_family (tenant_id, device_id, family),
  CONSTRAINT fk_network_interface_addresses_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT fk_network_interface_addresses_device FOREIGN KEY (device_id) REFERENCES network_devices(id) ON DELETE CASCADE,
  CONSTRAINT fk_network_interface_addresses_port FOREIGN KEY (port_id) REFERENCES network_ports(id) ON DELETE CASCADE,
  CHECK (family IN ('ipv4', 'ipv6')),
  CHECK ((family = 'ipv4' AND prefix_length <= 32) OR (family = 'ipv6' AND prefix_length <= 128))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
