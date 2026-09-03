-- Watchdog backend schema for the MySQL + VictoriaMetrics architecture.
-- MySQL owns business metadata, tenancy, permissions, SNMP configuration,
-- export jobs, and audit logs. High-cardinality metric samples stay in
-- VictoriaMetrics and reference these stable IDs as labels.

CREATE TABLE IF NOT EXISTS watchdog_installation (
  id VARCHAR(64) PRIMARY KEY,
  installed BOOLEAN NOT NULL DEFAULT TRUE,
  lock_path VARCHAR(512) NOT NULL DEFAULT '',
  config_path VARCHAR(512) NOT NULL DEFAULT '',
  installed_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS watchdog_schema_migrations (
  version VARCHAR(32) PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  checksum CHAR(64) NOT NULL,
  applied_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS tenants (
  id CHAR(26) PRIMARY KEY,
  name VARCHAR(190) NOT NULL,
  status VARCHAR(32) NOT NULL DEFAULT 'active',
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_tenants_name (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS users (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  email VARCHAR(320) NOT NULL,
  name VARCHAR(190) NOT NULL DEFAULT '',
  status VARCHAR(32) NOT NULL DEFAULT 'active',
  auth_provider VARCHAR(32) NULL,
  external_subject_id VARCHAR(64) NULL,
  password_hash VARCHAR(255) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_users_tenant_email (tenant_id, email),
  UNIQUE KEY uq_users_external_identity (auth_provider, external_subject_id, tenant_id),
  KEY idx_users_external_identity (auth_provider, external_subject_id),
  KEY idx_users_tenant_status (tenant_id, status),
  CONSTRAINT fk_users_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS roles (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  name VARCHAR(190) NOT NULL,
  scope VARCHAR(32) NOT NULL DEFAULT 'tenant',
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_roles_tenant_name (tenant_id, name),
  CONSTRAINT fk_roles_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS user_roles (
  user_id CHAR(26) NOT NULL,
  role_id CHAR(26) NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (user_id, role_id),
  CONSTRAINT fk_user_roles_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
  CONSTRAINT fk_user_roles_role FOREIGN KEY (role_id) REFERENCES roles(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS targets (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  name VARCHAR(190) NOT NULL,
  kind VARCHAR(32) NOT NULL,
  host VARCHAR(255) NOT NULL,
  mgmt_ip VARBINARY(16) NULL,
  status VARCHAR(32) NOT NULL DEFAULT 'pending',
  labels_json JSON NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  KEY idx_targets_tenant_kind_status (tenant_id, kind, status),
  CONSTRAINT fk_targets_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS target_credentials (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  target_id CHAR(26) NOT NULL,
  credential_type VARCHAR(32) NOT NULL,
  secret_ref VARCHAR(255) NOT NULL,
  config_json JSON NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  KEY idx_target_credentials_target (tenant_id, target_id, credential_type),
  CONSTRAINT fk_target_credentials_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT fk_target_credentials_target FOREIGN KEY (target_id) REFERENCES targets(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS target_agents (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  target_id CHAR(26) NOT NULL,
  agent_type VARCHAR(32) NOT NULL,
  mode VARCHAR(16) NOT NULL,
  endpoint VARCHAR(512) NULL,
  token_hash VARCHAR(255) NOT NULL,
  status VARCHAR(32) NOT NULL DEFAULT 'pending',
  last_seen_at DATETIME(3) NULL,
  last_run_at DATETIME(3) NULL,
  last_success_at DATETIME(3) NULL,
  last_error TEXT NULL,
  run_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
  failure_count BIGINT UNSIGNED NOT NULL DEFAULT 0,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  KEY idx_target_agents_target (tenant_id, target_id, agent_type),
  KEY idx_target_agents_status (tenant_id, status, last_seen_at),
  KEY idx_target_agents_run (agent_type, mode, status, last_run_at, failure_count),
  CONSTRAINT fk_target_agents_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT fk_target_agents_target FOREIGN KEY (target_id) REFERENCES targets(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS agent_run_history (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  agent_id CHAR(26) NOT NULL,
  target_id CHAR(26) NOT NULL,
  status VARCHAR(16) NOT NULL,
  error TEXT NULL,
  seen BOOLEAN NOT NULL DEFAULT FALSE,
  started_at DATETIME(3) NOT NULL,
  ended_at DATETIME(3) NOT NULL,
  duration_ms BIGINT UNSIGNED NOT NULL DEFAULT 0,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  KEY idx_agent_run_history_agent (tenant_id, agent_id, ended_at),
  KEY idx_agent_run_history_status (tenant_id, status, ended_at),
  CONSTRAINT fk_agent_run_history_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT fk_agent_run_history_agent FOREIGN KEY (agent_id) REFERENCES target_agents(id) ON DELETE CASCADE,
  CONSTRAINT fk_agent_run_history_target FOREIGN KEY (target_id) REFERENCES targets(id) ON DELETE CASCADE,
  CHECK (status IN ('success', 'failure'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS snmp_profiles (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  name VARCHAR(190) NOT NULL,
  version VARCHAR(8) NOT NULL,
  security_json JSON NOT NULL,
  timeout_ms INT UNSIGNED NOT NULL DEFAULT 3000,
  retries TINYINT UNSIGNED NOT NULL DEFAULT 2,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_snmp_profiles_tenant_name (tenant_id, name),
  CONSTRAINT fk_snmp_profiles_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS network_devices (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  target_id CHAR(26) NOT NULL,
  vendor VARCHAR(128) NULL,
  model VARCHAR(128) NULL,
  platform VARCHAR(190) NOT NULL DEFAULT '',
  os_name VARCHAR(128) NOT NULL DEFAULT '',
  os_version VARCHAR(128) NOT NULL DEFAULT '',
  sys_object_id VARCHAR(255) NULL,
  sys_name VARCHAR(255) NULL,
  sys_descr TEXT NULL,
  sys_location VARCHAR(255) NOT NULL DEFAULT '',
  uptime_seconds BIGINT UNSIGNED NOT NULL DEFAULT 0,
  snmp_profile_id CHAR(26) NULL,
  snmp_port SMALLINT UNSIGNED NOT NULL DEFAULT 161,
  snmp_security_json JSON NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_network_devices_target (tenant_id, target_id),
  KEY idx_network_devices_profile (snmp_profile_id),
  CONSTRAINT fk_network_devices_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT fk_network_devices_target FOREIGN KEY (target_id) REFERENCES targets(id) ON DELETE CASCADE,
  CONSTRAINT fk_network_devices_snmp_profile FOREIGN KEY (snmp_profile_id) REFERENCES snmp_profiles(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS network_ports (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  device_id CHAR(26) NOT NULL,
  if_index BIGINT UNSIGNED NOT NULL,
  if_name VARCHAR(190) NOT NULL DEFAULT '',
  if_alias VARCHAR(512) NOT NULL DEFAULT '',
  if_descr VARCHAR(512) NOT NULL DEFAULT '',
  admin_status VARCHAR(32) NOT NULL DEFAULT 'unknown',
  oper_status VARCHAR(32) NOT NULL DEFAULT 'unknown',
  speed_bps BIGINT UNSIGNED NOT NULL DEFAULT 0,
  metadata_json JSON NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_network_ports_if_index (tenant_id, device_id, if_index),
  KEY idx_network_ports_device_status (tenant_id, device_id, oper_status),
  CONSTRAINT fk_network_ports_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT fk_network_ports_device FOREIGN KEY (device_id) REFERENCES network_devices(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS port_policies (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  port_id CHAR(26) NOT NULL,
  side_type VARCHAR(16) NOT NULL,
  billing_base_bps BIGINT UNSIGNED NOT NULL,
  sample_step_seconds SMALLINT UNSIGNED NOT NULL,
  correction_direction VARCHAR(16) NOT NULL DEFAULT 'none',
  correction_min BIGINT NOT NULL DEFAULT 0,
  correction_max BIGINT NOT NULL DEFAULT 0,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_port_policies_port (tenant_id, port_id),
  CONSTRAINT fk_port_policies_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT fk_port_policies_port FOREIGN KEY (port_id) REFERENCES network_ports(id) ON DELETE CASCADE,
  CHECK (side_type IN ('provider', 'customer')),
  CHECK (sample_step_seconds IN (60, 300)),
  CHECK (correction_direction IN ('none', 'up', 'down')),
  CHECK (correction_min >= 0),
  CHECK (correction_max >= correction_min)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS network_port_transceivers (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  port_id CHAR(26) NOT NULL,
  module_type VARCHAR(128) NOT NULL DEFAULT '',
  vendor VARCHAR(128) NOT NULL DEFAULT '',
  model VARCHAR(128) NOT NULL DEFAULT '',
  serial VARCHAR(128) NOT NULL DEFAULT '',
  wavelength_nm INT UNSIGNED NULL,
  distance_m BIGINT UNSIGNED NULL,
  connector VARCHAR(64) NOT NULL DEFAULT '',
  raw_json JSON NULL,
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_port_transceivers_port (tenant_id, port_id),
  CONSTRAINT fk_port_transceivers_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT fk_port_transceivers_port FOREIGN KEY (port_id) REFERENCES network_ports(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS network_device_sensors (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  device_id CHAR(26) NOT NULL,
  port_id CHAR(26) NULL,
  sensor_index BIGINT UNSIGNED NOT NULL DEFAULT 0,
  sensor_class VARCHAR(64) NOT NULL,
  name VARCHAR(255) NOT NULL,
  oid VARCHAR(512) NOT NULL DEFAULT '',
  unit VARCHAR(64) NOT NULL DEFAULT '',
  current_value DOUBLE NULL,
  warn_limit DOUBLE NULL,
  crit_limit DOUBLE NULL,
  status VARCHAR(32) NOT NULL DEFAULT 'unknown',
  metadata_json JSON NULL,
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_network_device_sensors_index (tenant_id, device_id, sensor_index, sensor_class),
  KEY idx_network_device_sensors_device_class (tenant_id, device_id, sensor_class),
  KEY idx_network_device_sensors_port (tenant_id, port_id),
  CONSTRAINT fk_network_device_sensors_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT fk_network_device_sensors_device FOREIGN KEY (device_id) REFERENCES network_devices(id) ON DELETE CASCADE,
  CONSTRAINT fk_network_device_sensors_port FOREIGN KEY (port_id) REFERENCES network_ports(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS bgp_sessions (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  device_id CHAR(26) NOT NULL,
  peer_addr VARBINARY(16) NOT NULL,
  peer_as BIGINT UNSIGNED NOT NULL,
  local_as BIGINT UNSIGNED NOT NULL DEFAULT 0,
  afi VARCHAR(16) NOT NULL DEFAULT '',
  safi VARCHAR(16) NOT NULL DEFAULT '',
  state VARCHAR(32) NOT NULL DEFAULT 'unknown',
  accepted_prefixes BIGINT UNSIGNED NOT NULL DEFAULT 0,
  denied_prefixes BIGINT UNSIGNED NOT NULL DEFAULT 0,
  advertised_prefixes BIGINT UNSIGNED NOT NULL DEFAULT 0,
  uptime_seconds BIGINT UNSIGNED NOT NULL DEFAULT 0,
  metadata_json JSON NULL,
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_bgp_sessions_peer (tenant_id, device_id, peer_addr, peer_as, afi, safi),
  KEY idx_bgp_sessions_state (tenant_id, state),
  CONSTRAINT fk_bgp_sessions_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT fk_bgp_sessions_device FOREIGN KEY (device_id) REFERENCES network_devices(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS traffic_policy_defaults (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NULL,
  side_type VARCHAR(16) NOT NULL,
  billing_base_bps BIGINT UNSIGNED NOT NULL,
  sample_step_seconds SMALLINT UNSIGNED NOT NULL,
  correction_direction VARCHAR(16) NOT NULL DEFAULT 'none',
  correction_min BIGINT NOT NULL DEFAULT 0,
  correction_max BIGINT NOT NULL DEFAULT 0,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_traffic_policy_defaults_scope_side (tenant_id, side_type),
  CONSTRAINT fk_traffic_policy_defaults_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CHECK (side_type IN ('provider', 'customer')),
  CHECK (sample_step_seconds IN (60, 300)),
  CHECK (correction_direction IN ('none', 'up', 'down')),
  CHECK (correction_min >= 0),
  CHECK (correction_max >= correction_min)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS metric_retention_policies (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  target_id CHAR(26) NULL,
  high_precision_days INT UNSIGNED NOT NULL,
  manual_cleanup_enabled BOOLEAN NOT NULL DEFAULT TRUE,
  notes TEXT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_metric_retention_scope (tenant_id, target_id),
  CONSTRAINT fk_metric_retention_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT fk_metric_retention_target FOREIGN KEY (target_id) REFERENCES targets(id) ON DELETE CASCADE,
  CHECK (high_precision_days >= 1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS mib_modules (
  id CHAR(26) PRIMARY KEY,
  name VARCHAR(190) NOT NULL,
  source VARCHAR(64) NOT NULL DEFAULT 'librenms',
  version VARCHAR(64) NOT NULL DEFAULT '',
  checksum VARCHAR(128) NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_mib_modules_name_source (name, source)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS permissions (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  subject_type VARCHAR(16) NOT NULL,
  subject_id CHAR(26) NOT NULL,
  resource_type VARCHAR(32) NOT NULL,
  resource_id CHAR(26) NOT NULL,
  actions_json JSON NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_permissions_subject_resource (tenant_id, subject_type, subject_id, resource_type, resource_id),
  KEY idx_permissions_resource (tenant_id, resource_type, resource_id),
  CONSTRAINT fk_permissions_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CHECK (subject_type IN ('user', 'role')),
  CHECK (resource_type IN ('tenant', 'target', 'port', 'export_task', 'billing_account', 'billing_period'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS export_tasks (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  created_by CHAR(26) NOT NULL,
  target_id CHAR(26) NULL,
  port_id CHAR(26) NULL,
  period_type VARCHAR(16) NOT NULL,
  range_start DATETIME(3) NOT NULL,
  range_end DATETIME(3) NOT NULL,
  step_seconds INT UNSIGNED NOT NULL DEFAULT 300,
  aggregation VARCHAR(32) NOT NULL,
  value_mode VARCHAR(16) NOT NULL DEFAULT 'corrected',
  format VARCHAR(16) NOT NULL DEFAULT 'csv',
  status VARCHAR(32) NOT NULL DEFAULT 'pending',
  file_ref VARCHAR(512) NULL,
  error_message TEXT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  KEY idx_export_tasks_tenant_created (tenant_id, created_at),
  KEY idx_export_tasks_status (tenant_id, status),
  CONSTRAINT fk_export_tasks_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT fk_export_tasks_user FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE RESTRICT,
  CONSTRAINT fk_export_tasks_target FOREIGN KEY (target_id) REFERENCES targets(id) ON DELETE SET NULL,
  CONSTRAINT fk_export_tasks_port FOREIGN KEY (port_id) REFERENCES network_ports(id) ON DELETE SET NULL,
  CHECK (period_type IN ('day', 'month', 'fixed', 'custom')),
  CHECK (aggregation IN ('p95_5m', 'avg_5m', 'fourth_peak_5m', 'daily_p95', 'daily_avg', 'total_bytes')),
  CHECK (value_mode IN ('corrected', 'raw', 'both')),
  CHECK (format IN ('csv'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS billing_accounts (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  name VARCHAR(190) NOT NULL,
  status VARCHAR(32) NOT NULL DEFAULT 'active',
  billing_day TINYINT UNSIGNED NOT NULL DEFAULT 1,
  aggregation VARCHAR(32) NOT NULL DEFAULT 'p95_5m',
  value_mode VARCHAR(16) NOT NULL DEFAULT 'corrected',
  quota_bytes BIGINT UNSIGNED NULL,
  notes TEXT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_billing_accounts_tenant_name (tenant_id, name),
  CONSTRAINT fk_billing_accounts_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CHECK (billing_day BETWEEN 1 AND 28),
  CHECK (aggregation IN ('p95_5m', 'avg_5m', 'fourth_peak_5m', 'total_bytes')),
  CHECK (value_mode IN ('corrected', 'raw', 'both'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS billing_account_ports (
  billing_account_id CHAR(26) NOT NULL,
  tenant_id CHAR(26) NOT NULL,
  port_id CHAR(26) NOT NULL,
  direction VARCHAR(16) NOT NULL DEFAULT 'max',
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (billing_account_id, port_id),
  KEY idx_billing_account_ports_port (tenant_id, port_id),
  CONSTRAINT fk_billing_account_ports_account FOREIGN KEY (billing_account_id) REFERENCES billing_accounts(id) ON DELETE CASCADE,
  CONSTRAINT fk_billing_account_ports_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT fk_billing_account_ports_port FOREIGN KEY (port_id) REFERENCES network_ports(id) ON DELETE CASCADE,
  CHECK (direction IN ('in', 'out', 'sum', 'max'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS billing_periods (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  billing_account_id CHAR(26) NOT NULL,
  range_start DATETIME(3) NOT NULL,
  range_end DATETIME(3) NOT NULL,
  status VARCHAR(32) NOT NULL DEFAULT 'open',
  computed_value DOUBLE NULL,
  total_bytes BIGINT UNSIGNED NULL,
  computed_at DATETIME(3) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_billing_periods_account_range (billing_account_id, range_start, range_end),
  KEY idx_billing_periods_tenant_status (tenant_id, status),
  CONSTRAINT fk_billing_periods_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT fk_billing_periods_account FOREIGN KEY (billing_account_id) REFERENCES billing_accounts(id) ON DELETE CASCADE,
  CHECK (status IN ('open', 'computed', 'approved', 'void'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

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

CREATE TABLE IF NOT EXISTS aggregate_graphs (
  id VARCHAR(64) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  name VARCHAR(190) NOT NULL,
  aggregation VARCHAR(32) NOT NULL DEFAULT 'sum',
  value_mode VARCHAR(16) NOT NULL DEFAULT 'corrected',
  unit VARCHAR(64) NOT NULL DEFAULT '',
  description TEXT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_aggregate_graphs_tenant_name (tenant_id, name),
  KEY idx_aggregate_graphs_tenant (tenant_id),
  CONSTRAINT fk_aggregate_graphs_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CHECK (aggregation IN ('sum', 'avg', 'max', 'min', 'count')),
  CHECK (value_mode IN ('corrected', 'raw', 'both'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Data sources (metric + direction) for an aggregate graph. Mirrors Cacti's
-- per-data-source model: a traffic graph has one item for in_bps and one for
-- out_bps; each item is aggregated across the graph's port membership and,
-- when total=TRUE, contributes to the combined total.
CREATE TABLE IF NOT EXISTS aggregate_graph_items (
  id VARCHAR(64) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  aggregate_graph_id VARCHAR(64) NOT NULL,
  sequence INT UNSIGNED NOT NULL DEFAULT 0,
  metric VARCHAR(190) NOT NULL,
  direction VARCHAR(16) NOT NULL DEFAULT 'other',
  label VARCHAR(190) NOT NULL DEFAULT '',
  graph_type VARCHAR(16) NOT NULL DEFAULT 'line',
  total BOOLEAN NOT NULL DEFAULT FALSE,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  KEY idx_aggregate_graph_items_graph (tenant_id, aggregate_graph_id, sequence),
  CONSTRAINT fk_aggregate_graph_items_graph FOREIGN KEY (aggregate_graph_id) REFERENCES aggregate_graphs(id) ON DELETE CASCADE,
  CONSTRAINT fk_aggregate_graph_items_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CHECK (direction IN ('in', 'out', 'other')),
  CHECK (graph_type IN ('line', 'area', 'stack'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS aggregate_graph_ports (
  aggregate_graph_id VARCHAR(64) NOT NULL,
  tenant_id CHAR(26) NOT NULL,
  port_id CHAR(26) NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (aggregate_graph_id, port_id),
  KEY idx_aggregate_graph_ports_port (tenant_id, port_id),
  CONSTRAINT fk_aggregate_graph_ports_graph FOREIGN KEY (aggregate_graph_id) REFERENCES aggregate_graphs(id) ON DELETE CASCADE,
  CONSTRAINT fk_aggregate_graph_ports_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT fk_aggregate_graph_ports_port FOREIGN KEY (port_id) REFERENCES network_ports(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Immutable, append-only stored aggregate per item per timestamp. Written by
-- the rollup worker and frozen on first write (ON DUPLICATE KEY no-op) so the
-- snapshot stays auditable. Raw VM data remains the source of truth.
CREATE TABLE IF NOT EXISTS aggregate_graph_data (
  id VARCHAR(64) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  aggregate_graph_id VARCHAR(64) NOT NULL,
  item_id CHAR(26) NOT NULL,
  timestamp DATETIME(3) NOT NULL,
  value DOUBLE NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_aggregate_graph_data_ts (tenant_id, aggregate_graph_id, item_id, timestamp),
  KEY idx_aggregate_graph_data_graph (tenant_id, aggregate_graph_id, timestamp),
  CONSTRAINT fk_aggregate_graph_data_graph FOREIGN KEY (aggregate_graph_id) REFERENCES aggregate_graphs(id) ON DELETE CASCADE,
  CONSTRAINT fk_aggregate_graph_data_item FOREIGN KEY (item_id) REFERENCES aggregate_graph_items(id) ON DELETE CASCADE,
  CONSTRAINT fk_aggregate_graph_data_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS audit_logs (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  actor_id CHAR(26) NULL,
  action VARCHAR(190) NOT NULL,
  resource_type VARCHAR(32) NOT NULL,
  resource_id CHAR(26) NULL,
  ip VARBINARY(16) NULL,
  detail_json JSON NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  KEY idx_audit_logs_tenant_created (tenant_id, created_at),
  KEY idx_audit_logs_resource (tenant_id, resource_type, resource_id),
  CONSTRAINT fk_audit_logs_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT fk_audit_logs_actor FOREIGN KEY (actor_id) REFERENCES users(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
