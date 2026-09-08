-- Watchdog v2 auth extension: password reset tokens (self-service "forgot password").
-- Only the SHA-256 of the reset token is stored; single-use, time-bounded.

CREATE TABLE IF NOT EXISTS password_reset_tokens (
  id           CHAR(26)    NOT NULL,
  user_id      CHAR(26)    NOT NULL,
  token_sha256 CHAR(64)    NOT NULL,
  expires_at   DATETIME(3) NOT NULL,
  used_at      DATETIME(3) NULL,
  created_at   DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_prt_token (token_sha256),
  KEY idx_prt_user (user_id),
  CONSTRAINT fk_prt_user FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
