-- Single-domain SNMP metric-retention policy storage. This preserves the
-- established management API while removing its former tenant ownership.
CREATE TABLE IF NOT EXISTS metric_retention_policies (
  id                     CHAR(26)     NOT NULL,
  scope_key              VARCHAR(26)  NOT NULL,
  target_id              CHAR(26)     NULL,
  high_precision_days    INT UNSIGNED NOT NULL,
  manual_cleanup_enabled TINYINT(1)   NOT NULL DEFAULT 1,
  notes                  TEXT         NULL,
  created_at             DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at             DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_metric_retention_scope (scope_key),
  KEY idx_metric_retention_target (target_id),
  CONSTRAINT fk_metric_retention_target FOREIGN KEY (target_id) REFERENCES devices(id) ON DELETE CASCADE,
  CHECK (high_precision_days >= 1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
