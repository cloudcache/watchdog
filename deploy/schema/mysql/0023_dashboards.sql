-- Dashboards are management-plane presentation definitions. They are global
-- within this single-domain installation; owner_id is attribution/filtering,
-- not a tenant boundary. Panels keep stable references to saved SNMP graphs.
CREATE TABLE IF NOT EXISTS dashboards (
  id          CHAR(26)     NOT NULL,
  owner_id    CHAR(26)     NULL,
  name        VARCHAR(190) NOT NULL,
  description TEXT         NULL,
  layout_json JSON         NOT NULL,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_dashboards_name (name),
  KEY idx_dashboards_owner_updated (owner_id,updated_at),
  CONSTRAINT fk_dashboards_owner FOREIGN KEY (owner_id) REFERENCES users(id) ON DELETE SET NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
