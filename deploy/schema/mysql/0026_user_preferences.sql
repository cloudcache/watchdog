-- User UI preferences: one JSON settings row per user under optimistic
-- concurrency (row_version -> ETag / If-Match). De-tenant port of the legacy
-- internal/watchdog user_preferences store; drops tenant_id (single install).
CREATE TABLE IF NOT EXISTS user_preferences (
  user_id       CHAR(26)         NOT NULL,
  settings_json JSON             NOT NULL,
  row_version   BIGINT UNSIGNED  NOT NULL DEFAULT 1,
  updated_at    DATETIME(3)      NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (user_id),
  CONSTRAINT fk_user_preferences_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
