-- KISS-06 phase-1c: reusable canonical Flow filter ASTs (saved filters).
-- De-tenanted port of the hub flow_saved_filters table: no tenant_id column;
-- share_scope collapses the old private/tenant pair to private/shared, where
-- "shared" means visible to every user of this single-tenant install.
CREATE TABLE IF NOT EXISTS flow_saved_filters (
  id                    CHAR(26)         NOT NULL,
  owner_user_id         CHAR(26)         NULL,
  name                  VARCHAR(190)     NOT NULL,
  description           VARCHAR(1024)    NOT NULL DEFAULT '',
  share_scope           VARCHAR(16)      NOT NULL DEFAULT 'private',
  filter_schema_version SMALLINT UNSIGNED NOT NULL DEFAULT 1,
  filter_json           JSON             NOT NULL,
  row_version           BIGINT UNSIGNED  NOT NULL DEFAULT 1,
  created_at            DATETIME(3)      NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at            DATETIME(3)      NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  deleted_at            DATETIME(3)      NULL DEFAULT NULL,
  PRIMARY KEY (id),
  KEY idx_flow_saved_filters_visibility (deleted_at, share_scope, owner_user_id),
  KEY idx_flow_saved_filters_owner (owner_user_id),
  KEY idx_flow_saved_filters_updated (updated_at),
  CONSTRAINT fk_flow_saved_filters_owner FOREIGN KEY (owner_user_id) REFERENCES users(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
