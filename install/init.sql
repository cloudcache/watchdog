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
  UNIQUE KEY uq_targets_tenant_kind_host (tenant_id, kind, host),
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

CREATE TABLE IF NOT EXISTS collector_agents (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  module_key VARCHAR(64) NOT NULL,
  name VARCHAR(190) NOT NULL,
  agent_type VARCHAR(64) NOT NULL,
  mode VARCHAR(16) NOT NULL,
  endpoint VARCHAR(512) NULL,
  status VARCHAR(24) NOT NULL DEFAULT 'pending',
  observed_health VARCHAR(24) NOT NULL DEFAULT 'unknown',
  auth_type VARCHAR(16) NOT NULL DEFAULT 'token',
  token_hash VARCHAR(255) NULL,
  certificate_fingerprint VARCHAR(190) NULL,
  capabilities_json JSON NULL,
  capabilities_hash CHAR(64) NOT NULL DEFAULT '',
  capability_schema_version SMALLINT UNSIGNED NOT NULL DEFAULT 1,
  software_version VARCHAR(64) NOT NULL DEFAULT '',
  agent_api_version SMALLINT UNSIGNED NOT NULL DEFAULT 1,
  plan_schema_min SMALLINT UNSIGNED NOT NULL DEFAULT 1,
  plan_schema_max SMALLINT UNSIGNED NOT NULL DEFAULT 1,
  boot_id VARCHAR(64) NOT NULL DEFAULT '',
  runtime_schema_version SMALLINT UNSIGNED NOT NULL DEFAULT 0,
  heartbeat_sequence BIGINT UNSIGNED NOT NULL DEFAULT 0,
  heartbeat_sent_at DATETIME(3) NULL,
  clock_offset_ms BIGINT NULL,
  runtime_observation_json JSON NULL,
  runtime_observation_hash CHAR(64) NOT NULL DEFAULT '',
  heartbeat_payload_hash CHAR(64) NOT NULL DEFAULT '',
  config_version BIGINT UNSIGNED NOT NULL DEFAULT 0,
  acknowledged_config_version BIGINT UNSIGNED NOT NULL DEFAULT 0,
  last_good_config_version BIGINT UNSIGNED NOT NULL DEFAULT 0,
  plan_hash CHAR(64) NOT NULL DEFAULT '',
  plan_expires_at DATETIME(3) NULL,
  last_seen_at DATETIME(3) NULL,
  last_error_code VARCHAR(64) NULL,
  last_error_detail VARCHAR(1024) NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_by CHAR(26) NOT NULL,
  updated_by CHAR(26) NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  deleted_at DATETIME(3) NULL,
  purge_after DATETIME(3) NULL,
  live_identity TINYINT GENERATED ALWAYS AS (IF(deleted_at IS NULL, 1, NULL)) STORED,
  UNIQUE KEY uq_collector_agent_tenant_id (tenant_id, id),
  UNIQUE KEY uq_collector_name (tenant_id, module_key, name, live_identity),
  KEY idx_collector_health (tenant_id, status, observed_health, last_seen_at),
  CONSTRAINT fk_collector_agent_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CHECK (mode IN ('push','pull','listen')),
  CHECK (status IN ('pending','active','suspended','revoked','deleted')),
  CHECK ((status = 'deleted') = (deleted_at IS NOT NULL)),
  CHECK (observed_health IN ('unknown','warming','healthy','degraded','stale','unavailable')),
  CHECK (plan_schema_min <= plan_schema_max),
  CHECK (last_good_config_version <= acknowledged_config_version),
  CHECK (acknowledged_config_version <= config_version),
  CHECK (
    (runtime_schema_version = 0 AND heartbeat_sequence = 0 AND heartbeat_sent_at IS NULL AND clock_offset_ms IS NULL
      AND runtime_observation_json IS NULL AND runtime_observation_hash = '' AND heartbeat_payload_hash = '')
    OR
    (runtime_schema_version > 0 AND heartbeat_sequence > 0 AND heartbeat_sent_at IS NOT NULL AND clock_offset_ms IS NOT NULL
      AND capabilities_json IS NOT NULL AND CHAR_LENGTH(capabilities_hash) = 64
      AND runtime_observation_json IS NOT NULL AND CHAR_LENGTH(runtime_observation_hash) = 64
      AND CHAR_LENGTH(heartbeat_payload_hash) = 64)
  ),
  CHECK (auth_type IN ('token','mtls')),
  CHECK ((auth_type = 'token') = (token_hash IS NOT NULL)),
  CHECK ((auth_type = 'mtls') = (certificate_fingerprint IS NOT NULL))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS collector_bindings (
  collector_id CHAR(26) NOT NULL,
  tenant_id CHAR(26) NOT NULL,
  resource_type VARCHAR(64) NOT NULL,
  resource_id CHAR(26) NOT NULL,
  binding_role VARCHAR(64) NOT NULL DEFAULT 'collect',
  config_json JSON NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_by CHAR(26) NOT NULL,
  updated_by CHAR(26) NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (collector_id, resource_type, resource_id),
  KEY idx_collector_binding_resource (tenant_id, resource_type, resource_id, binding_role),
  CONSTRAINT fk_collector_binding_collector FOREIGN KEY (tenant_id, collector_id) REFERENCES collector_agents(tenant_id, id) ON DELETE CASCADE,
  CONSTRAINT fk_collector_binding_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS collector_plan_revisions (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  collector_id CHAR(26) NOT NULL,
  config_version BIGINT UNSIGNED NOT NULL,
  plan_schema_version SMALLINT UNSIGNED NOT NULL,
  status VARCHAR(16) NOT NULL DEFAULT 'draft',
  spec_json JSON NOT NULL,
  spec_hash CHAR(64) NOT NULL,
  signing_key_id VARCHAR(64) NULL,
  signature VARBINARY(512) NULL,
  validation_json JSON NULL,
  not_before DATETIME(3) NULL,
  expires_at DATETIME(3) NULL,
  supersedes_config_version BIGINT UNSIGNED NULL,
  created_by CHAR(26) NOT NULL,
  updated_by CHAR(26) NOT NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  activated_at DATETIME(3) NULL,
  retired_at DATETIME(3) NULL,
  active_identity TINYINT GENERATED ALWAYS AS (IF(status = 'active', 1, NULL)) STORED,
  UNIQUE KEY uq_collector_plan_version (collector_id, config_version),
  UNIQUE KEY uq_collector_plan_tenant_version (tenant_id, collector_id, config_version),
  KEY idx_collector_plan_hash (collector_id, spec_hash),
  KEY idx_collector_plan_status (collector_id, status, config_version),
  UNIQUE KEY uq_collector_active_plan (collector_id, active_identity),
  CONSTRAINT fk_collector_plan_collector FOREIGN KEY (tenant_id, collector_id) REFERENCES collector_agents(tenant_id, id) ON DELETE CASCADE,
  CONSTRAINT fk_collector_plan_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CHECK (status IN ('draft','validated','active','retired','rejected')),
  CHECK (config_version > 0),
  CHECK (plan_schema_version > 0),
  CHECK (status <> 'active' OR (signature IS NOT NULL AND activated_at IS NOT NULL AND expires_at IS NOT NULL)),
  CHECK (status <> 'retired' OR retired_at IS NOT NULL),
  CHECK (expires_at IS NULL OR not_before IS NULL OR expires_at > not_before),
  CHECK (supersedes_config_version IS NULL OR supersedes_config_version < config_version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS collector_service_principals (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  collector_id CHAR(26) NOT NULL,
  service_type VARCHAR(32) NOT NULL,
  principal_ref VARCHAR(190) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  credential_secret_ref VARCHAR(255) NOT NULL,
  provider VARCHAR(64) NOT NULL,
  status VARCHAR(16) NOT NULL DEFAULT 'active',
  grant_receipt_ref VARCHAR(512) NOT NULL,
  grant_receipt_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  write_revoked_at DATETIME(3) NULL,
  revoke_receipt_ref VARCHAR(512) NULL,
  revoke_receipt_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
  acl_propagation_delay_ms BIGINT UNSIGNED NOT NULL,
  created_by CHAR(26) NOT NULL,
  updated_by CHAR(26) NOT NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_collector_service_principal_tenant_id (tenant_id, id),
  UNIQUE KEY uq_collector_service_principal_ref (service_type, principal_ref),
  KEY idx_collector_service_principal_owner (tenant_id, collector_id, service_type, status),
  CONSTRAINT fk_collector_service_principal_collector FOREIGN KEY (tenant_id, collector_id) REFERENCES collector_agents(tenant_id, id) ON DELETE RESTRICT,
  CONSTRAINT fk_collector_service_principal_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT fk_collector_service_principal_creator FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE RESTRICT,
  CONSTRAINT fk_collector_service_principal_updater FOREIGN KEY (updated_by) REFERENCES users(id) ON DELETE RESTRICT,
  CHECK (service_type IN ('kafka')),
  CHECK (status IN ('active','revoked')),
  CHECK ((status = 'revoked') = (write_revoked_at IS NOT NULL)),
  CHECK ((status = 'revoked') = (revoke_receipt_ref IS NOT NULL)),
  CHECK ((status = 'revoked') = (revoke_receipt_sha256 IS NOT NULL))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS collector_ownership_transfers (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  exporter_id CHAR(26) NOT NULL,
  old_collector_id CHAR(26) NOT NULL,
  new_collector_id CHAR(26) NOT NULL,
  old_plan_revision BIGINT UNSIGNED NOT NULL,
  old_revoke_plan_revision BIGINT UNSIGNED NOT NULL,
  new_plan_revision BIGINT UNSIGNED NOT NULL,
  old_ownership_epoch BIGINT UNSIGNED NOT NULL,
  new_ownership_epoch BIGINT UNSIGNED NOT NULL,
  old_principal_id CHAR(26) NOT NULL,
  max_clock_skew_ms BIGINT UNSIGNED NOT NULL,
  approval_id CHAR(26) NOT NULL,
  requested_by CHAR(26) NOT NULL,
  old_owner_boot_id VARCHAR(64) NULL,
  old_owner_drain_config_version BIGINT UNSIGNED NULL,
  old_owner_drained_at DATETIME(3) NULL,
  drain_receipt_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_collector_ownership_transfer_tenant_id (tenant_id, id),
  UNIQUE KEY uq_collector_ownership_transfer_epoch (tenant_id, exporter_id, old_ownership_epoch),
  KEY idx_collector_ownership_transfer_new (tenant_id, exporter_id, new_ownership_epoch),
  CONSTRAINT fk_collector_ownership_transfer_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT fk_collector_ownership_transfer_old_collector FOREIGN KEY (tenant_id, old_collector_id) REFERENCES collector_agents(tenant_id, id) ON DELETE RESTRICT,
  CONSTRAINT fk_collector_ownership_transfer_new_collector FOREIGN KEY (tenant_id, new_collector_id) REFERENCES collector_agents(tenant_id, id) ON DELETE RESTRICT,
  CONSTRAINT fk_collector_ownership_transfer_old_plan FOREIGN KEY (tenant_id, old_collector_id, old_plan_revision) REFERENCES collector_plan_revisions(tenant_id, collector_id, config_version) ON DELETE RESTRICT,
  CONSTRAINT fk_collector_ownership_transfer_old_revoke_plan FOREIGN KEY (tenant_id, old_collector_id, old_revoke_plan_revision) REFERENCES collector_plan_revisions(tenant_id, collector_id, config_version) ON DELETE RESTRICT,
  CONSTRAINT fk_collector_ownership_transfer_new_plan FOREIGN KEY (tenant_id, new_collector_id, new_plan_revision) REFERENCES collector_plan_revisions(tenant_id, collector_id, config_version) ON DELETE RESTRICT,
  CONSTRAINT fk_collector_ownership_transfer_principal FOREIGN KEY (tenant_id, old_principal_id) REFERENCES collector_service_principals(tenant_id, id) ON DELETE RESTRICT,
  CONSTRAINT fk_collector_ownership_transfer_requester FOREIGN KEY (requested_by) REFERENCES users(id) ON DELETE RESTRICT,
  CHECK (old_collector_id <> new_collector_id),
  CHECK (old_plan_revision > 0),
  CHECK (old_revoke_plan_revision > old_plan_revision),
  CHECK (new_plan_revision > 0),
  CHECK (old_ownership_epoch > 0),
  CHECK (new_ownership_epoch > old_ownership_epoch),
  CHECK ((old_owner_drained_at IS NULL) = (old_owner_boot_id IS NULL)),
  CHECK ((old_owner_drained_at IS NULL) = (old_owner_drain_config_version IS NULL)),
  CHECK ((old_owner_drained_at IS NULL) = (drain_receipt_sha256 IS NULL))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS collector_state_restore_receipts (
  transfer_id CHAR(26) NOT NULL,
  tenant_id CHAR(26) NOT NULL,
  state_kind VARCHAR(16) NOT NULL,
  state_identity_key BINARY(32) NOT NULL,
  new_owner_boot_id VARCHAR(64) NOT NULL,
  restore_config_version BIGINT UNSIGNED NOT NULL,
  restored_old_ownership_epoch BIGINT UNSIGNED NOT NULL,
  restored_old_generation BIGINT UNSIGNED NOT NULL,
  new_epoch_baseline_generation BIGINT UNSIGNED NOT NULL,
  receipt_sha256 CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  reported_at DATETIME(3) NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (transfer_id, state_kind, state_identity_key),
  KEY idx_collector_state_restore_identity (tenant_id, state_kind, state_identity_key),
  CONSTRAINT fk_collector_state_restore_transfer FOREIGN KEY (tenant_id, transfer_id) REFERENCES collector_ownership_transfers(tenant_id, id) ON DELETE CASCADE,
  CONSTRAINT fk_collector_state_restore_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CHECK (state_kind IN ('decoder','quality')),
  CHECK (restore_config_version > 0),
  CHECK (restored_old_ownership_epoch > 0),
  CHECK (restored_old_generation > 0)
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
  UNIQUE KEY uq_network_interface_addresses (tenant_id, device_id, if_index, address, prefix_length, context_name),
  KEY idx_network_interface_addresses_port (tenant_id, port_id, family),
  KEY idx_network_interface_addresses_device_family (tenant_id, device_id, family),
  CONSTRAINT fk_network_interface_addresses_tenant FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT fk_network_interface_addresses_device FOREIGN KEY (device_id) REFERENCES network_devices(id) ON DELETE CASCADE,
  CONSTRAINT fk_network_interface_addresses_port FOREIGN KEY (port_id) REFERENCES network_ports(id) ON DELETE CASCADE,
  CHECK (family IN ('ipv4', 'ipv6')),
  CHECK ((family = 'ipv4' AND prefix_length <= 32) OR (family = 'ipv6' AND prefix_length <= 128))
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

CREATE TABLE IF NOT EXISTS operation_jobs (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  job_type VARCHAR(64) NOT NULL,
  status VARCHAR(16) NOT NULL DEFAULT 'queued',
  idempotency_key VARCHAR(128) NOT NULL,
  request_hash CHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
  progress_total BIGINT UNSIGNED NULL,
  progress_done BIGINT UNSIGNED NOT NULL DEFAULT 0,
  checkpoint_json JSON NOT NULL,
  result_ref VARCHAR(1024) NULL,
  lease_owner VARCHAR(128) NULL,
  lease_token VARCHAR(64) NULL,
  lease_expires_at DATETIME(3) NULL,
  next_attempt_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  attempt_count INT UNSIGNED NOT NULL DEFAULT 0,
  last_error_code VARCHAR(64) NULL,
  last_error_detail VARCHAR(1024) NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_by CHAR(26) NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  started_at DATETIME(3) NULL,
  heartbeat_at DATETIME(3) NULL,
  cancel_requested_at DATETIME(3) NULL,
  finished_at DATETIME(3) NULL,
  expires_at DATETIME(3) NULL,
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
    ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_operation_jobs_idempotency
    (tenant_id, job_type, idempotency_key),
  KEY idx_operation_jobs_due
    (job_type, status, next_attempt_at, lease_expires_at, id),
  KEY idx_operation_jobs_tenant_created (tenant_id, job_type, created_at, id),
  CONSTRAINT fk_operation_jobs_tenant
    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT fk_operation_jobs_creator
    FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE SET NULL,
  CHECK (status IN ('queued','running','paused','validating','cancel_requested','succeeded','failed','canceled')),
  CHECK (progress_total IS NULL OR progress_done <= progress_total),
  CHECK ((lease_owner IS NULL) = (lease_token IS NULL)),
  CHECK ((lease_owner IS NULL) = (lease_expires_at IS NULL)),
  CHECK ((status IN ('running','validating','cancel_requested')) =
    (lease_owner IS NOT NULL)),
  CHECK ((status IN ('succeeded','failed','canceled')) =
    (finished_at IS NOT NULL))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
