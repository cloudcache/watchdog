-- PLAT P0 storage consolidation (alert subsystem, step 3): alert history moves
-- out of the PocketBase alerts_history collection into MySQL. The alert engine
-- still lives in PocketBase and fires create/resolve on the alerts collection's
-- record events; those events now write here, resolving the PocketBase user id
-- to the MySQL user via users.external_subject_id.
--
-- alert_id is the PocketBase alerts record id (the history row's parent);
-- system_id is the PocketBase system id the alert is for ('' if none). value is
-- the threshold value captured at trigger. resolved_at is NULL while the alert
-- is still active and set to the resolve time when it clears.
CREATE TABLE IF NOT EXISTS alerts_history (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  user_id CHAR(26) NOT NULL,
  alert_id VARCHAR(255) NOT NULL,
  system_id VARCHAR(255) NOT NULL DEFAULT '',
  name VARCHAR(255) NOT NULL,
  value DOUBLE NOT NULL DEFAULT 0,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  resolved_at DATETIME(3) NULL,
  KEY idx_alerts_history_user (tenant_id, user_id, created_at, id),
  KEY idx_alerts_history_alert (alert_id, resolved_at),
  CONSTRAINT fk_alerts_history_user FOREIGN KEY (user_id)
    REFERENCES users(id) ON DELETE CASCADE,
  CONSTRAINT fk_alerts_history_tenant FOREIGN KEY (tenant_id)
    REFERENCES tenants(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
