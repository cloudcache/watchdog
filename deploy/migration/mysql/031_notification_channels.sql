-- PLAT P0 storage consolidation (alert subsystem, step 1): notification
-- channels (email addresses, webhook URLs) move out of the PocketBase
-- user_settings blob into MySQL. One row per channel; the alert delivery path
-- reads them by resolving the PocketBase user id to the MySQL user via
-- users.external_subject_id.
--
-- Webhook URLs are stored in `address` as plaintext for now, matching the
-- current PocketBase behaviour; moving webhook secrets into a secret store is
-- a follow-up. channel_type constrains the row to a known kind.
CREATE TABLE IF NOT EXISTS notification_channels (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  user_id CHAR(26) NOT NULL,
  channel_type VARCHAR(16) NOT NULL,
  address VARCHAR(1024) NOT NULL,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
    ON UPDATE CURRENT_TIMESTAMP(3),
  KEY idx_notification_channels_user (tenant_id, user_id, enabled),
  CONSTRAINT fk_notification_channels_user FOREIGN KEY (user_id)
    REFERENCES users(id) ON DELETE CASCADE,
  CONSTRAINT fk_notification_channels_tenant FOREIGN KEY (tenant_id)
    REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT chk_notification_channel_type CHECK (channel_type IN ('email', 'webhook'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
