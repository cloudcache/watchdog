-- PLAT P0 storage consolidation: move user preferences out of the PocketBase
-- `user_settings` collection into MySQL so PB shrinks toward an auth kernel.
-- settings_json currently holds the full preference blob (UI prefs plus the
-- legacy email/webhook notification lists) as a transitional state; the
-- email/webhook channels are extracted into a dedicated notification_channels
-- table (with a secret store for webhooks) in a follow-up slice.
--
-- One row per user; row_version drives optimistic concurrency (If-Match).
CREATE TABLE IF NOT EXISTS user_preferences (
  user_id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  schema_version SMALLINT UNSIGNED NOT NULL DEFAULT 1,
  settings_json JSON NOT NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
    ON UPDATE CURRENT_TIMESTAMP(3),
  KEY idx_user_preferences_tenant (tenant_id, updated_at),
  CONSTRAINT fk_user_preferences_user FOREIGN KEY (user_id)
    REFERENCES users(id) ON DELETE CASCADE,
  CONSTRAINT fk_user_preferences_tenant FOREIGN KEY (tenant_id)
    REFERENCES tenants(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
