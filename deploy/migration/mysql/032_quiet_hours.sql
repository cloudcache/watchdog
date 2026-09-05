-- PLAT P0 storage consolidation (alert subsystem, step 2): quiet-hour windows
-- move out of the PocketBase quiet_hours collection into MySQL. The alert
-- silencing path reads them by resolving the PocketBase user id to the MySQL
-- user via users.external_subject_id.
--
-- system_id is the PocketBase system id the window applies to; '' means the
-- window is global (all systems for the user). window_type is 'daily' (only
-- the HH:MM of start/end matters, recurring) or 'one-time' (the full datetime
-- range applies once).
CREATE TABLE IF NOT EXISTS quiet_hours (
  id CHAR(26) PRIMARY KEY,
  tenant_id CHAR(26) NOT NULL,
  user_id CHAR(26) NOT NULL,
  system_id VARCHAR(255) NOT NULL DEFAULT '',
  window_type VARCHAR(16) NOT NULL,
  start_at DATETIME(3) NOT NULL,
  end_at DATETIME(3) NOT NULL,
  created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
    ON UPDATE CURRENT_TIMESTAMP(3),
  KEY idx_quiet_hours_user (tenant_id, user_id),
  KEY idx_quiet_hours_user_system (tenant_id, user_id, system_id),
  CONSTRAINT fk_quiet_hours_user FOREIGN KEY (user_id)
    REFERENCES users(id) ON DELETE CASCADE,
  CONSTRAINT fk_quiet_hours_tenant FOREIGN KEY (tenant_id)
    REFERENCES tenants(id) ON DELETE CASCADE,
  CONSTRAINT chk_quiet_hours_type CHECK (window_type IN ('daily', 'one-time'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
