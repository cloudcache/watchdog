-- SNMP traffic presentation policy and custom MIB metadata are low-frequency
-- management objects. Raw counters and derived rate samples remain in ClickHouse.
CREATE TABLE IF NOT EXISTS traffic_policy_defaults (
  id                   CHAR(26)        NOT NULL,
  side_type            VARCHAR(16)     NOT NULL,
  billing_base_bps     BIGINT UNSIGNED NOT NULL,
  sample_step_seconds  SMALLINT UNSIGNED NOT NULL,
  correction_direction VARCHAR(16)     NOT NULL DEFAULT 'none',
  correction_min       BIGINT          NOT NULL DEFAULT 0,
  correction_max       BIGINT          NOT NULL DEFAULT 0,
  row_version          BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_at           DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at           DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_traffic_policy_defaults_side (side_type),
  CHECK (side_type IN ('provider','customer')),
  CHECK (sample_step_seconds IN (60,300)),
  CHECK (correction_direction IN ('none','up','down')),
  CHECK (correction_min >= 0),
  CHECK (correction_max >= correction_min)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS port_policies (
  id                   CHAR(26)        NOT NULL,
  port_id              CHAR(26)        NOT NULL,
  side_type            VARCHAR(16)     NOT NULL,
  billing_base_bps     BIGINT UNSIGNED NOT NULL,
  sample_step_seconds  SMALLINT UNSIGNED NOT NULL,
  correction_direction VARCHAR(16)     NOT NULL DEFAULT 'none',
  correction_min       BIGINT          NOT NULL DEFAULT 0,
  correction_max       BIGINT          NOT NULL DEFAULT 0,
  enabled              TINYINT(1)      NOT NULL DEFAULT 1,
  row_version          BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_at           DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at           DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_port_policies_port (port_id),
  CONSTRAINT fk_port_policies_port FOREIGN KEY (port_id) REFERENCES ports(id) ON DELETE CASCADE,
  CHECK (side_type IN ('provider','customer')),
  CHECK (sample_step_seconds IN (60,300)),
  CHECK (correction_direction IN ('none','up','down')),
  CHECK (correction_min >= 0),
  CHECK (correction_max >= correction_min)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE TABLE IF NOT EXISTS mib_modules (
  id          CHAR(26)     NOT NULL,
  name        VARCHAR(190) NOT NULL,
  source      VARCHAR(64)  NOT NULL DEFAULT 'librenms',
  version     VARCHAR(64)  NOT NULL DEFAULT '',
  checksum    VARCHAR(128) NOT NULL,
  enabled     TINYINT(1)   NOT NULL DEFAULT 1,
  row_version BIGINT UNSIGNED NOT NULL DEFAULT 1,
  created_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  updated_at  DATETIME(3)  NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
  PRIMARY KEY (id),
  UNIQUE KEY uq_mib_modules_name_source (name,source)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
