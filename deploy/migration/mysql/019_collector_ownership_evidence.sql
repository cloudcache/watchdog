ALTER TABLE collector_plan_revisions
  ADD UNIQUE KEY uq_collector_plan_tenant_version
    (tenant_id, collector_id, config_version);

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
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
    ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_collector_service_principal_tenant_id (tenant_id, id),
  UNIQUE KEY uq_collector_service_principal_ref (service_type, principal_ref),
  KEY idx_collector_service_principal_owner
    (tenant_id, collector_id, service_type, status),
  CONSTRAINT fk_collector_service_principal_collector
    FOREIGN KEY (tenant_id, collector_id)
    REFERENCES collector_agents(tenant_id, id) ON DELETE RESTRICT,
  CONSTRAINT fk_collector_service_principal_tenant
    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT fk_collector_service_principal_creator
    FOREIGN KEY (created_by) REFERENCES users(id) ON DELETE RESTRICT,
  CONSTRAINT fk_collector_service_principal_updater
    FOREIGN KEY (updated_by) REFERENCES users(id) ON DELETE RESTRICT,
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
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
    ON UPDATE CURRENT_TIMESTAMP(3),
  UNIQUE KEY uq_collector_ownership_transfer_tenant_id (tenant_id, id),
  UNIQUE KEY uq_collector_ownership_transfer_epoch
    (tenant_id, exporter_id, old_ownership_epoch),
  KEY idx_collector_ownership_transfer_new
    (tenant_id, exporter_id, new_ownership_epoch),
  CONSTRAINT fk_collector_ownership_transfer_tenant
    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT fk_collector_ownership_transfer_old_collector
    FOREIGN KEY (tenant_id, old_collector_id)
    REFERENCES collector_agents(tenant_id, id) ON DELETE RESTRICT,
  CONSTRAINT fk_collector_ownership_transfer_new_collector
    FOREIGN KEY (tenant_id, new_collector_id)
    REFERENCES collector_agents(tenant_id, id) ON DELETE RESTRICT,
  CONSTRAINT fk_collector_ownership_transfer_old_plan
    FOREIGN KEY (tenant_id, old_collector_id, old_plan_revision)
    REFERENCES collector_plan_revisions(tenant_id, collector_id, config_version)
    ON DELETE RESTRICT,
  CONSTRAINT fk_collector_ownership_transfer_old_revoke_plan
    FOREIGN KEY (tenant_id, old_collector_id, old_revoke_plan_revision)
    REFERENCES collector_plan_revisions(tenant_id, collector_id, config_version)
    ON DELETE RESTRICT,
  CONSTRAINT fk_collector_ownership_transfer_new_plan
    FOREIGN KEY (tenant_id, new_collector_id, new_plan_revision)
    REFERENCES collector_plan_revisions(tenant_id, collector_id, config_version)
    ON DELETE RESTRICT,
  CONSTRAINT fk_collector_ownership_transfer_principal
    FOREIGN KEY (tenant_id, old_principal_id)
    REFERENCES collector_service_principals(tenant_id, id) ON DELETE RESTRICT,
  CONSTRAINT fk_collector_ownership_transfer_requester
    FOREIGN KEY (requested_by) REFERENCES users(id) ON DELETE RESTRICT,
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
  KEY idx_collector_state_restore_identity
    (tenant_id, state_kind, state_identity_key),
  CONSTRAINT fk_collector_state_restore_transfer
    FOREIGN KEY (tenant_id, transfer_id)
    REFERENCES collector_ownership_transfers(tenant_id, id) ON DELETE CASCADE,
  CONSTRAINT fk_collector_state_restore_tenant
    FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
  CHECK (state_kind IN ('decoder','quality')),
  CHECK (restore_config_version > 0),
  CHECK (restored_old_ownership_epoch > 0),
  CHECK (restored_old_generation > 0)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
